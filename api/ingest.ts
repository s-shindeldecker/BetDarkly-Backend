/*
  Snowflake DDL for the analytics events table:

  CREATE TABLE IF NOT EXISTS ANALYTICS_EVENTS (
    MESSAGE_ID       VARCHAR(36)    NOT NULL,
    EVENT_TIMESTAMP  TIMESTAMP_NTZ  NOT NULL,
    EVENT_TYPE       VARCHAR(20)    NOT NULL,
    EVENT_NAME       VARCHAR(255)   NOT NULL,
    USER_ID          VARCHAR(255),
    ANONYMOUS_ID     VARCHAR(36),
    SESSION_ID       VARCHAR(36),
    PROPERTIES       VARIANT,
    CONTEXT          VARIANT,
    INSERTED_AT      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
  );
*/

import type { VercelRequest, VercelResponse } from '@vercel/node';
import snowflake from 'snowflake-sdk';

const ALLOWED_ORIGINS = new Set([
  'https://cap1-betdarkly.lovable.app',
  'https://id-preview--74ea028b-83e5-4ba8-b5f6-d7b551b4f447.lovable.app',
  'http://localhost:5173',
  'http://localhost:8080',
]);

const CORS_HEADERS = {
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
} as const;

function setCors(res: VercelResponse, origin: string | undefined): void {
  if (origin && ALLOWED_ORIGINS.has(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
  }
  res.setHeader('Access-Control-Allow-Headers', CORS_HEADERS['Access-Control-Allow-Headers']);
  res.setHeader('Access-Control-Allow-Methods', CORS_HEADERS['Access-Control-Allow-Methods']);
}

interface IngestEvent {
  message_id: string;
  timestamp: string;
  event_type: string;
  event_name: string;
  user_id?: string;
  anonymous_id?: string;
  session_id?: string;
  properties?: Record<string, unknown>;
  context?: Record<string, unknown>;
}

function validateEvent(e: unknown, index: number): e is IngestEvent {
  if (!e || typeof e !== 'object' || Array.isArray(e)) {
    throw new Error(`events[${index}]: must be an object`);
  }
  const o = e as Record<string, unknown>;
  if (typeof o.message_id !== 'string' || !o.message_id.trim()) {
    throw new Error(`events[${index}]: message_id must be a non-empty string`);
  }
  if (typeof o.timestamp !== 'string' || !o.timestamp.trim()) {
    throw new Error(`events[${index}]: timestamp must be a non-empty string`);
  }
  if (typeof o.event_type !== 'string' || !o.event_type.trim()) {
    throw new Error(`events[${index}]: event_type must be a non-empty string`);
  }
  if (typeof o.event_name !== 'string' || !o.event_name.trim()) {
    throw new Error(`events[${index}]: event_name must be a non-empty string`);
  }
  return true;
}

function getConnection(): snowflake.Connection {
  const account = process.env.SNOWFLAKE_ACCOUNT;
  const user = process.env.SNOWFLAKE_USER;
  const password = process.env.SNOWFLAKE_PASSWORD;
  const privateKeyRaw = process.env.SNOWFLAKE_PRIVATE_KEY;
  const privateKeyPass = process.env.SNOWFLAKE_PRIVATE_KEY_PASS;
  const warehouse = process.env.SNOWFLAKE_WAREHOUSE;
  const database = process.env.SNOWFLAKE_DATABASE;
  const schema = process.env.SNOWFLAKE_SCHEMA;

  if (!account || !user || !warehouse || !database || !schema) {
    throw new Error(
      'Missing Snowflake config: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA must be set'
    );
  }

  const useKeyPair = Boolean(privateKeyRaw?.trim());

  if (!useKeyPair && !password) {
    throw new Error(
      'Set either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY for Snowflake authentication'
    );
  }

  const base = {
    account,
    username: user,
    warehouse,
    database,
    schema,
  };

  if (useKeyPair) {
    // PEM in env often has newlines as literal \n; normalize for the SDK
    const privateKey = privateKeyRaw!.replace(/\\n/g, '\n').trim();
    return snowflake.createConnection({
      ...base,
      authenticator: 'SNOWFLAKE_JWT',
      privateKey,
      ...(privateKeyPass ? { privateKeyPass } : {}),
    });
  }

  return snowflake.createConnection({
    ...base,
    password: password!,
  });
}

export default async function handler(req: VercelRequest, res: VercelResponse): Promise<void> {
  const origin = req.headers.origin;
  setCors(res, origin);

  if (req.method === 'OPTIONS') {
    res.status(204).end();
    return;
  }

  if (req.method !== 'POST') {
    res.status(405).json({ error: 'Method not allowed' });
    return;
  }

  const auth = req.headers.authorization;
  const expected = process.env.SNOWFLAKE_PROXY_API_KEY;
  if (!auth || !auth.startsWith('Bearer ') || !expected) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }
  const token = auth.slice(7);
  if (token !== expected) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }

  let body: { events?: unknown[] };
  try {
    body = typeof req.body === 'object' && req.body !== null ? (req.body as { events?: unknown[] }) : {};
  } catch {
    res.status(400).json({ error: 'Invalid JSON body' });
    return;
  }

  const rawEvents = body.events;
  if (!Array.isArray(rawEvents) || rawEvents.length === 0) {
    res.status(400).json({ error: 'events must be a non-empty array' });
    return;
  }

  let events: IngestEvent[];
  try {
    events = rawEvents.filter((e, i) => {
      validateEvent(e, i);
      return true;
    }) as IngestEvent[];
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Validation failed';
    res.status(400).json({ error: message });
    return;
  }

  try {
    const connection = getConnection();

    await new Promise<void>((resolve, reject) => {
      connection.connect((err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    const sql = `
    INSERT INTO ANALYTICS_EVENTS (
      MESSAGE_ID,
      EVENT_TIMESTAMP,
      EVENT_TYPE,
      EVENT_NAME,
      USER_ID,
      ANONYMOUS_ID,
      SESSION_ID,
      PROPERTIES,
      CONTEXT
    ) VALUES (?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?))
  `;

    const binds = events.map((ev) => [
      ev.message_id,
      ev.timestamp,
      ev.event_type,
      ev.event_name,
      ev.user_id ?? null,
      ev.anonymous_id ?? null,
      ev.session_id ?? null,
      JSON.stringify(ev.properties ?? {}),
      JSON.stringify(ev.context ?? {}),
    ]);

    await new Promise<void>((resolve, reject) => {
      connection.execute({
        sqlText: sql,
        binds: binds as snowflake.Bind[],
        complete: (err) => {
          if (err) reject(err);
          else resolve();
        },
      });
    });

    await new Promise<void>((resolve, reject) => {
      connection.destroy((err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    res.status(200).json({ inserted: events.length });
  } catch {
    res.status(500).json({ error: 'Internal server error' });
  }
}
