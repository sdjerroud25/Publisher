// mqtt-subscriber-seeder.ts
import "dotenv/config";
import mqtt, { MqttClient } from "mqtt";
import fs from "fs/promises";
import path from "path";

interface Mapping {
  tag: string;
  topic: string;
}

const MQTT_URL: string = process.env.MQTT_BROKER_URL || "mqtt://192.168.0.211:1883";
const CONFIG_DIR: string = path.join(__dirname, "config");

async function loadMappings(): Promise<Mapping[]> {
  const files = await fs.readdir(CONFIG_DIR);
  const mappings: Mapping[] = [];

  for (const file of files) {
    if (!file.endsWith(".json")) continue;
    const raw = await fs.readFile(path.join(CONFIG_DIR, file), "utf-8");
    let arr: unknown[] = [];
    try { arr = JSON.parse(raw); }
    catch (e: any) {
      console.error(`❌ Invalid JSON in ${file}: ${e.message}`);
      continue;
    }
    console.log(`✅ Loaded ${file}:`, arr);
    for (const item of arr) {
      if (typeof item === "string") {
        mappings.push({ tag: item, topic: item });
      } else if (
        typeof item === "object" &&
        item !== null &&
        "topic" in item &&
        typeof (item as any).topic === "string"
      ) {
        const t = item as { tag?: string; topic: string };
        mappings.push({ tag: t.tag ?? t.topic, topic: t.topic });
      } else {
        console.warn(`⚠️ Skipping invalid entry in ${file}:`, item);
      }
    }
  }

  return mappings;
}

async function main(): Promise<void> {
  const mappings = await loadMappings();
  const client: MqttClient = mqtt.connect(MQTT_URL, { reconnectPeriod: 1000 });

  client.on("connect", () => {
    console.log(`✅ Connected to MQTT broker at ${MQTT_URL}`);
    const seenTopics = new Set<string>();

    for (const { tag, topic } of mappings) {
      if
