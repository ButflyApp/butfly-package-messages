import { connect } from "amqplib";
import pino from "pino";
import { IMessagingPublisherAdapter } from "../contracts/IMessagingPublisher";
import { v4 as uuidv4 } from "uuid";

const ID_KEYS = [
  "uid",
  "id",
  "habitarProcessUid",
  "requeueUid",
  "companyUid",
  "userUid",
];

const pickIdentifier = (payload: any): string => {
  if (!payload || typeof payload !== "object") return "unknown";
  for (const key of ID_KEYS) {
    if (payload[key]) return `${key}=${payload[key]}`;
  }
  return "no-id";
};

export class RabbitPublisherAdapter implements IMessagingPublisherAdapter {
  private logger: pino.Logger<never, boolean>;
  private defaultConfig = { autoSetOrigin: true };
  constructor({ logger }: { logger: pino.Logger<never, boolean> }) {
    this.logger = logger;
  }

  async sendMessage({
    exchange,
    queue,
    message,
    config = this.defaultConfig,
  }: {
    exchange?: string;
    queue?: string;
    message: any;
    config?: { autoSetOrigin?: boolean };
  }) {
    if (config.autoSetOrigin) message.messageOrigin = process.env.SERVICE_NAME;
    if (!message.habitarProcessUid) message.habitarProcessUid = uuidv4();

    const target = exchange ? `exchange "${exchange}"` : `queue "${queue}"`;
    const identifier = pickIdentifier(message);

    try {
      const connection = await connect({
        heartbeat: 5,
        hostname: process.env.AMQP_CONNECTION,
        username: process.env.AMQP_USER,
        password: process.env.AMQP_PASS,
      });
      const channel = await connection.createChannel();

      if (exchange) {
        await channel.assertExchange(exchange, "fanout");
        channel.publish(exchange, "", Buffer.from(JSON.stringify(message)));
        this.logger.info(
          {
            exchange,
            habitarProcessUid: message.habitarProcessUid,
            messageOrigin: message.messageOrigin,
            payload: message,
          },
          `Published message to exchange "${exchange}" (${identifier})`
        );
      } else if (queue) {
        channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)));
        this.logger.info(
          {
            queue,
            habitarProcessUid: message.habitarProcessUid,
            messageOrigin: message.messageOrigin,
            payload: message,
          },
          `Published message to queue "${queue}" (${identifier})`
        );
      }

      await channel.close();
      await connection.close();
    } catch (error: any) {
      this.logger.error(
        {
          err: { message: error?.message, stack: error?.stack },
          target,
          habitarProcessUid: message?.habitarProcessUid,
          identifier,
        },
        `Unexpected error publishing message to ${target} (${identifier}): ${error?.message ?? "unknown error"}`
      );
      throw error;
    }
  }
}
