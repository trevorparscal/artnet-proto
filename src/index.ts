import { createSocket, type Socket } from "node:dgram";
import { EventEmitter } from "node:events";

interface ArtnetConfig {
    host?: string;
    port?: number;
    refresh?: number;
    sendAll?: boolean;
    interface?: string;
}

/**
 * Represents a discovered Art-Net node (device, which may be a fixture, gateway, or controller).
 */
interface ArtNetNodeInfo {
    shortName: string;
    longName: string;
    nodeIp: string;
    portCount: number;

    /**
     * First input port’s full 15-bit universe (Net<<8 | Sub<<4 | Chan) for quick access
     */
    universe: number;

    /**
     * Up to 4 input universes, one per input port (computed from Net/Sub + SwIn[0..3])
     */
    universesIn: number[];

    /**
     * Up to 4 output universes, one per output port (computed from Net/Sub + SwOut[0..3])
     */
    universesOut: number[];
}

type ArtnetCallback = (error: Error | null, bytes?: number) => void;

/**
 * Implements the Art-Net v4 protocol for DMX-over-IP communication.
 * Reference: https://art-net.org.uk/downloads/art-net.pdf
 */
export class Artnet extends EventEmitter {
    private host: string;
    private port: number;
    private refresh: number;
    private sendAll: boolean;
    private socket: Socket;
    private data: number[][] = [];
    private interval: NodeJS.Timeout[] = [];
    private sendThrottle: (NodeJS.Timeout | undefined)[] = [];
    private sendDelayed: boolean[] = [];
    private dataChanged: number[] = [];
    private discoveredNodes: {
        ip: string;
        port: number;
        info: ArtNetNodeInfo;
    }[] = [];

    constructor(config: ArtnetConfig = {}) {
        super();
        this.host = config.host ?? "255.255.255.255";
        this.port = config.port ?? 6454;
        this.refresh = config.refresh ?? 4000;
        this.sendAll = config.sendAll ?? false;

        this.socket = createSocket({ type: "udp4", reuseAddr: true });

        this.socket.on("error", (err) => {
            this.emit("error", err);
        });

        if (config.interface && this.host === "255.255.255.255") {
            this.socket.bind(this.port, config.interface, () => {
                this.socket.setBroadcast(true);
            });
        } else if (this.host.endsWith(".255")) {
            this.socket.bind(this.port, () => {
                this.socket.setBroadcast(true);
            });
        }
    }

    /**
     * Send an Art-Net trigger packet.
     * Reference: Art-Net 4 Spec, p40
     */
    public sendTrigger(
        oem: number,
        key: number,
        subKey: number,
        callback?: ArtnetCallback,
    ): void {
        const buf = this.triggerPackage(oem, key, subKey);
        this.socket.send(buf, 0, buf.length, this.port, this.host, callback);
    }

    private triggerPackage(oem: number, key: number, subkey: number): Buffer {
        // See Art-Net 4, p40 for structure
        const hOem = (oem >> 8) & 0xff;
        const lOem = oem & 0xff;
        const header = [
            65,
            114,
            116,
            45,
            78,
            101,
            116,
            0,
            0,
            153,
            0,
            14,
            0,
            0,
            hOem,
            lOem,
            key,
            subkey,
        ];
        const payload = Array(512).fill(0);
        return Buffer.from(header.concat(payload));
    }

    private startRefresh(universe: number): void {
        this.interval[universe] = setInterval(() => {
            this.send(universe, 512);
        }, this.refresh);
    }

    /**
     * Send DMX data as ArtDmx packet.
     * Reference: Art-Net 4 Spec, p45 (ArtDmx Packet Definition)
     */
    public send(
        universe: number,
        refresh: number | boolean = false,
        callback?: ArtnetCallback,
    ): void {
        // Handle overload: send(universe, callback)
        if (typeof refresh === "function") {
            callback = refresh;
            refresh = false;
        }

        // Global override to always send all channels
        if (this.sendAll) {
            refresh = true;
        }

        // Start refresh timer for this universe if needed
        if (!this.interval[universe]) {
            this.startRefresh(universe);
        }

        // Handle throttling
        if (this.sendThrottle[universe]) {
            this.sendDelayed[universe] = true;
            return;
        }

        clearTimeout(this.sendThrottle[universe]);

        // Capture args in closure for the throttle timeout
        const sendRefresh = refresh;
        const sendCallback = callback;

        this.sendThrottle[universe] = setTimeout(() => {
            this.sendThrottle[universe] = undefined;
            if (this.sendDelayed[universe]) {
                this.sendDelayed[universe] = false;
                this.send(universe, sendRefresh, sendCallback);
            }
        }, 25);

        // Enforce minimum length
        let length: number;
        if (refresh) {
            length = 512;
        } else {
            // If no changes recorded, still send full 512 for safety
            length =
                this.dataChanged[universe] && this.dataChanged[universe] > 0
                    ? this.dataChanged[universe]
                    : 512;
        }

        // Build packet and reset change counter
        const buf = this.artdmxPackage(universe, length);
        this.dataChanged[universe] = 0;

        // Send the packet
        this.socket.send(
            buf,
            0,
            buf.length,
            this.port,
            this.host,
            (err, bytes) => {
                if (typeof callback === "function") {
                    callback(
                        err,
                        typeof bytes === "number" ? bytes : buf.length,
                    );
                }
            },
        );
    }

    private artdmxPackage(universe: number, length: number = 2): Buffer {
        // Length must be even per Art-Net spec
        if (length % 2) {
            length++;
        }

        const hUni = (universe >> 8) & 0xff;
        const lUni = universe & 0xff;
        const hLen = (length >> 8) & 0xff;
        const lLen = length & 0xff;

        // Art-Net 4, p45 (ArtDmx Packet)
        const header = [
            65,
            114,
            116,
            45,
            78,
            101,
            116,
            0, // "Art-Net" + null terminator
            0,
            80, // OpCode = OpOutput / OpDmx (0x5000 little endian)
            0,
            14, // Protocol Version (14 decimal)
            0,
            0, // Sequence + Physical (set to zero for auto)
            lUni,
            hUni, // Universe (little endian)
            hLen,
            lLen, // Length (DMX length)
        ];

        if (!this.data[universe]) {
            this.data[universe] = Array(512).fill(0);
        }

        const lengthBytes = hLen * 256 + lLen;

        // Ensure we only send numbers 0–255 (replace null with 0)
        const cleanDmx = this.data[universe]
            .slice(0, lengthBytes)
            .map((value) => value ?? 0);

        return Buffer.from(header.concat(cleanDmx));
    }

    public set(
        universe: number,
        channel: number,
        value: number,
        callback?: ArtnetCallback,
    ): boolean;
    public set(
        universe: number,
        channel: number,
        values: number[],
        callback?: ArtnetCallback,
    ): boolean;
    public set(
        channel: number,
        value: number,
        callback?: ArtnetCallback,
    ): boolean;
    public set(
        channel: number,
        values: number[],
        callback?: ArtnetCallback,
    ): boolean;
    public set(value: number, callback?: ArtnetCallback): boolean;
    public set(values: number[], callback?: ArtnetCallback): boolean;

    /**
     * Set DMX channel(s) to value(s). Can be single value or array.
     * @example set(1, 1, 255) // universe 1, channel 1 = 255
     */
    public set(
        arg1: number | number[],
        arg2?: number | number[] | ArtnetCallback,
        arg3?: number | number[] | ArtnetCallback,
        arg4?: ArtnetCallback,
    ): boolean {
        let universe: number = 0;
        let channel: number = 1;
        let value: number | number[];
        let callback: ArtnetCallback | undefined;

        if (
            typeof arg1 === "number" &&
            typeof arg2 === "number" &&
            (typeof arg3 === "number" || Array.isArray(arg3))
        ) {
            // set(universe, channel, value/arr, [callback])
            universe = arg1;
            channel = arg2;
            value = arg3 as number | number[];
            callback = arg4;
        } else if (
            typeof arg1 === "number" &&
            (typeof arg2 === "number" || Array.isArray(arg2))
        ) {
            // set(channel, value/arr, [callback])
            channel = arg1;
            value = arg2 as number | number[];
            callback = arg3 as ArtnetCallback | undefined;
        } else {
            // set(value/arr, [callback])
            channel = 1;
            value = arg1 as number | number[];
            callback = arg2 as ArtnetCallback | undefined;
        }

        // Ensure universe exists
        if (!this.data[universe]) {
            this.data[universe] = Array(512).fill(0);
        }

        this.dataChanged[universe] = this.dataChanged[universe] || 0;

        // Update data
        if (Array.isArray(value)) {
            for (let i = 0; i < value.length; i++) {
                const index = channel + i - 1;
                if (
                    typeof value[i] === "number" &&
                    this.data[universe][index] !== value[i]
                ) {
                    this.data[universe][index] = value[i];
                    if (index + 1 > this.dataChanged[universe]) {
                        this.dataChanged[universe] = index + 1;
                    }
                }
            }
        } else if (
            typeof value === "number" &&
            this.data[universe][channel - 1] !== value
        ) {
            this.data[universe][channel - 1] = value;
            if (channel > this.dataChanged[universe]) {
                this.dataChanged[universe] = channel;
            }
        }

        // Send if changed
        if (this.dataChanged[universe]) {
            this.send(universe, false, callback);
        } else if (typeof callback === "function") {
            callback(null, undefined);
        }
        return true;
    }

    public trigger(
        oem: number,
        subKey: number,
        key: number,
        callback?: ArtnetCallback,
    ): boolean;
    public trigger(
        subKey: number,
        key: number,
        callback?: ArtnetCallback,
    ): boolean;
    public trigger(key: number, callback?: ArtnetCallback): boolean;

    /**
     * Send a trigger event.
     */
    public trigger(
        arg1: number,
        arg2?: number | ArtnetCallback,
        arg3?: number | ArtnetCallback,
        arg4?: ArtnetCallback,
    ): boolean {
        let oem: number;
        let subKey: number;
        let key: number;
        let callback: ArtnetCallback | undefined;

        if (typeof arg2 === "number" && typeof arg3 === "number") {
            // trigger(oem, subKey, key, [callback])
            oem = arg1;
            subKey = arg2;
            key = arg3;
            callback = arg4;
        } else if (typeof arg2 === "number" && typeof arg3 === "function") {
            // trigger(subKey, key, callback)
            oem = 0xffff;
            subKey = arg1;
            key = arg2;
            callback = arg3;
        } else if (typeof arg2 === "function") {
            // trigger(key, callback)
            oem = 0xffff;
            subKey = 0;
            key = arg1;
            callback = arg2;
        } else if (typeof arg2 === "number") {
            // trigger(subKey, key)
            oem = 0xffff;
            subKey = arg1;
            key = arg2;
        } else {
            // trigger(key)
            oem = 0xffff;
            subKey = 0;
            key = arg1;
        }

        // Defaults per Art-Net spec (p40)
        oem = oem ?? 0xffff;
        key = key ?? 255;

        this.sendTrigger(oem, key, subKey, callback);
        return true;
    }

    /**
     * Discovers all Art-Net nodes (devices) present on the local network.
     *
     * Broadcasts an ArtPoll message and collects all ArtPollReply responses for the timeout period.
     * Each discovered node provides IP address, short name, universes, and port info.
     *
     * @param timeout How long to wait for replies, in milliseconds (default: 2000ms).
     * @returns Promise resolving with an array of discovered nodes.
     */
    public discoverNodes(
        timeout: number = 2000,
    ): Promise<{ ip: string; port: number; info: ArtNetNodeInfo }[]> {
        return new Promise((resolve) => {
            const socket = createSocket({ type: 'udp4', reuseAddr: true } );
            this.discoveredNodes = [];

            // Handle incoming UDP messages (possible ArtPollReply packets)
            socket.on("message", (msg: Buffer, rinfo) => {
                if (msg.length < 12) {
                    return;
                }

                // Check "Art-Net\0"
                const isArtNet =
                    msg[0] === 0x41 &&
                    msg[1] === 0x72 &&
                    msg[2] === 0x74 &&
                    msg[3] === 0x2d &&
                    msg[4] === 0x4e &&
                    msg[5] === 0x65 &&
                    msg[6] === 0x74 &&
                    msg[7] === 0x00;

                // ArtPollReply opcode 0x2100 (little-endian: 0x00, 0x21 at 8,9)
                const isArtPollReply =
                    isArtNet && msg[8] === 0x00 && msg[9] === 0x21;

                if (!isArtPollReply) {
                    return;
                }

                const info = this.parseArtPollReply(msg);
                if (!this.discoveredNodes.some((n) => n.ip === rinfo.address)) {
                    this.discoveredNodes.push({
                        ip: rinfo.address,
                        port: rinfo.port,
                        info,
                    });
                }
            });

            // After timeout, stop listening and return discovered nodes
            setTimeout(() => {
                socket.close();
                resolve(this.discoveredNodes);
            }, timeout);

            // Bind socket to standard Art-Net port and broadcast ArtPoll
            socket.bind(6454, () => {
                socket.setBroadcast(true);
                const pollPacket = this.createArtPollPacket();
                socket.send(
                    pollPacket,
                    0,
                    pollPacket.length,
                    6454,
                    "255.255.255.255",
                );
            });
        });
    }

    /**
     * Generates a valid ArtPoll (discovery) UDP packet per Art-Net specification.
     *
     * @returns Buffer containing the ArtPoll packet.
     */
    private createArtPollPacket(): Buffer {
        const packet = Buffer.from( 'Art-Net\x000  \x0e\x00\x00', 'ascii' ); // OpPoll, ver 14
        packet.writeUInt16LE( 0x2000, 8 ); // OpPoll = 0x2000 (little-endian in packet)
        return packet;
    }

    /**
     * Extracts important information from an ArtPollReply UDP packet.
     *
     * Offsets (bytes):
     * -  0..7   : "Art-Net\0"
     * -  8..9   : OpCode (reply == 0x2100, little-endian)
     * - 10..13  : Node IP address
     * - 18      : NetSwitch (7 bits used, 0..127)
     * - 19      : SubSwitch (low nibble used, 0..15)
     * - 26..43  : ShortName (18 bytes, null-terminated ASCII)
     * - 44..107 : LongName (64 bytes, null-terminated ASCII)
     * - 172..173: NumPorts (Hi, Lo)
     * - 186..189: SwIn[0..3]  (low nibble == input universe/channel 0..15)
     * - 190..193: SwOut[0..3] (low nibble == output universe/channel 0..15)
     *
     * Universe number (15-bit) is composed as:
     *   universe = (NetSwitch << 8) | (SubSwitch << 4) | (PortNibble)
     * Where PortNibble is low-nibble of SwIn[x] or SwOut[x].
     *
     * @param msg Buffer containing the ArtPollReply packet.
     * @returns ArtNetNodeInfo object with extracted node/device details.
     */
    private parseArtPollReply(msg: Buffer): ArtNetNodeInfo {
        // Clean null-terminated ASCII strings and strip garbage (common on cheap nodes)
        function getString(start: number, length: number) {
            const end = msg.indexOf(0, start);
            const raw = msg.subarray(start, end === -1 ? start + length : end).toString("ascii");
            // eslint-disable-next-line no-control-regex
            return raw.replace(/[\x00-\x1F\x7F-\xFF]/g, "").trim();
        };

        const nodeIp =
            msg.length >= 14
                ? `${msg[10]}.${msg[11]}.${msg[12]}.${msg[13]}`
                : "0.0.0.0";

        const net = msg.length > 18 ? msg[18] & 0x7f : 0; // 0..127
        const sub = msg.length > 19 ? msg[19] & 0x0f : 0; // 0..15

        // Port count: NumPortsHi/Lo at 172/173
        const portCount = msg.length > 173 ? (msg[172] << 8) | msg[173] : 0;

        // SwIn[0..3] and SwOut[0..3] (guard against short packets)
        const swInBytes = msg.length >= 190 ? [...msg.subarray(186, 190)] : [];
        const swOutBytes = msg.length >= 194 ? [...msg.subarray(190, 194)] : [];

        // Compose full universes for each available port
        const compose = (nibble: number) =>
            (net << 8) | (sub << 4) | (nibble & 0x0f);
        const universesIn = swInBytes.map((b) => compose(b & 0x0f));
        const universesOut = swOutBytes.map((b) => compose(b & 0x0f));

        // Keep the “first input port” universe for backward compatibility
        const universe =
            universesIn[0] ?? compose(msg.length > 186 ? msg[186] & 0x0f : 0);

        return {
            shortName: getString(26, 18),
            longName: getString(44, 64),
            nodeIp,
            portCount,
            universe,
            universesIn,
            universesOut,
        };
    }

    /**
     * Stop all polling/refresh and close the socket.
     */
    public close(): void {
        for (const interval of this.interval) {
            if (interval) {
                clearInterval(interval);
            }
        }
        for (const throttle of this.sendThrottle) {
            if (throttle) {
                clearTimeout(throttle);
            }
        }
        this.socket.close();
    }

    /**
     * Change the Art-Net output host.
     */
    public setHost(host: string): void {
        this.host = host;
    }

    /**
     * Set a new UDP port. (Only allowed when not using broadcast)
     */
    public setPort(port: number): void {
        if (this.host === "255.255.255.255") {
            throw new Error(
                "Can't change port when using broadcast address 255.255.255.255",
            );
        } else {
            this.port = port;
        }
    }
}
