// mqtt-subscriber-seeder.js
"use strict";
require("dotenv").config();
const mqtt = require("mqtt");
const fs = require("fs/promises");
const path = require("path");

const MQTT_URL = process.env.MQTT_BROKER_URL || "mqtt://192.168.0.211:1883";
const CONFIG_DIR = path.join(__dirname, "config");

async function loadMappings() {
  const files = await fs.readdir(CONFIG_DIR);
  const mappings = [];

  for (const file of files) {
    if (!file.endsWith(".json")) continue;
    const raw = await fs.readFile(path.join(CONFIG_DIR, file), "utf-8");
    let arr = [];
    try { arr = JSON.parse(raw); }
    catch (e) {
      console.error(`❌ Invalid JSON in ${file}: ${e.message}`);
      continue;
    }
    console.log(`✅ Loaded ${file}:`, arr);
    arr.forEach(item => {
      if (typeof item === "string") {
        mappings.push({ tag: item, topic: item });
      } else if (item.topic && typeof item.topic === "string") {
        mappings.push({ tag: item.tag || item.topic, topic: item.topic });
      } else {
        console.warn(`⚠️ Skipping invalid entry in ${file}:`, item);
      }
    });
  }
  return mappings;
}

(async () => {
  const mappings = await loadMappings();
  const client = mqtt.connect(MQTT_URL, { reconnectPeriod: 1000 });

  client.on("connect", () => {
    console.log(`✅ Connected to MQTT broker at ${MQTT_URL}`);
    const seen = new Set();

    mappings.forEach(({ tag, topic }) => {
      if (seen.has(topic)) return;
      seen.add(topic);

      // Subscribe (wildcards are allowed)
      client.subscribe(topic, (err, granted) => {
        if (err) console.error(`❌ Subscribe failed for "${topic}":`, err.message);
        else console.log(`📡 Subscribed to "${topic}" [QoS ${granted[0]?.qos}]`);
      });

      // Publish a retained "init" message
      const initMsg = JSON.stringify({ tag, init: new Date().toISOString(), value: null });
      client.publish(topic, initMsg, { qos: 1, retain: true }, err => {
        if (err) console.error(`❌ Publish failed for "${topic}":`, err.message);
        else console.log(`📤 Retained init for "${topic}"`);
      });
    });
  });

  client.on("message", (topic, buf) => {
    if (!topic) return console.warn("⚠️ Received message with undefined topic");
    let payload;
    try { payload = JSON.parse(buf.toString()); }
    catch { payload = buf.toString(); }

    const mapping = mappings.find(m => m.topic === topic);
    const tag = mapping?.tag || topic;
    console.log(`📝 [${tag}] ${topic} →`, payload);
  });

  client.on("error", err => console.error("❌ MQTT error:", err.message));
})();
