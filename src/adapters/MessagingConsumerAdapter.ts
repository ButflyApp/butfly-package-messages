import { Channel, Connection, ConsumeMessage } from "amqplib";
import pino from "pino";
import { IMessagingConsumerAdapter } from "../contracts/IMessagingConsumer";
import { IMessagingPublisherAdapter } from "../contracts/IMessagingPublisher";
import { v4 } from "uuid";
import { ErrorHandler } from "./ErrorHandler";

type RabbitConsumerAdapterOptions = {
  exchange: string;
  queue: string;
  successMessage: string;
};

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

export class RabbitConsumerAdapter implements IMessagingConsumerAdapter {
  private useCase: { execute: (data: any) => Promise<any> | any };
  private options: RabbitConsumerAdapterOptions;
  private messagingPublisher: IMessagingPublisherAdapter;
  private logger: pino.Logger<never, boolean>;
  private amqpConnection: (data: {
    exchange: string;
    queues: string[];
  }) => Promise<{ channel: Channel; connection: Connection }>;
  private config: {
    requeue: boolean;
  };
  private errorHandler: ErrorHandler;

  constructor(
    {
      useCase,
      messagingPublisher,
      logger,
      amqpConnection,
      config = {},
    }: {
      useCase: { execute: (data: any) => Promise<any> | any };
      messagingPublisher: IMessagingPublisherAdapter;
      logger: pino.Logger<never, boolean>;
      amqpConnection: (data: {
        exchange: string;
        queues: string[];
      }) => Promise<{ channel: Channel; connection: Connection }>;
      config?: Partial<{
        requeue: boolean;
      }>;
    },
    options: RabbitConsumerAdapterOptions
  ) {
    this.useCase = useCase;
    this.options = options;
    this.messagingPublisher = messagingPublisher;
    this.logger = logger;
    this.amqpConnection = amqpConnection;
    this.config = {
      requeue: config?.requeue || false,
    };
    this.errorHandler = new ErrorHandler(logger);
    this.consume();
  }

  protected async consume() {
    const { queue, exchange, successMessage } = this.options;
    const queueName = `${process.env.SERVICE_NAME}.${queue}`;

    const { channel } = await this.amqpConnection({
      exchange,
      queues: [queueName],
    });

    this.logger.info(
      { exchange, queue: queueName },
      `Consumer is listening on queue "${queueName}" (exchange "${exchange}")`
    );

    await channel.consume(queueName, async (message) => {
      const queueMessage = message?.content?.toString();
      if (!queueMessage) return;

      const jsonQueueMessage: any = JSON.parse(queueMessage);

      if (Array.isArray(jsonQueueMessage)) {
        await this.consumeArray(channel, queueName, jsonQueueMessage, message);
      } else {
        await this.consumeObject(
          channel,
          jsonQueueMessage,
          message,
          queueName,
          successMessage
        );
      }
    });
  }

  private async consumeArray(
    channel: Channel,
    queueName: string,
    queueMessage: any,
    message: ConsumeMessage | null
  ) {
    this.logger.info(
      { queue: queueName, batchSize: queueMessage.length },
      `Received batch of ${queueMessage.length} messages on queue "${queueName}", splitting into individual messages`
    );

    for (const item of queueMessage) {
      await channel.sendToQueue(queueName, Buffer.from(JSON.stringify(item)));
    }
    channel.ack(message as ConsumeMessage);
  }

  private async consumeObject(
    channel: Channel,
    jsonObject: any,
    message: ConsumeMessage | null,
    queueName: string,
    successMessage: string
  ) {
    const identifier = pickIdentifier(jsonObject);
    const habitarProcessUid = jsonObject?.habitarProcessUid;

    this.logger.info(
      {
        queue: queueName,
        exchange: this.options.exchange,
        habitarProcessUid,
        messageOrigin: jsonObject?.messageOrigin,
        payload: jsonObject,
      },
      `Received message on queue "${queueName}" (${identifier})`
    );

    try {
      if (jsonObject.messageOrigin === process.env.SERVICE_NAME) {
        this.logger.info(
          { queue: queueName, habitarProcessUid, identifier },
          `Skipping self-originated message on queue "${queueName}" (${identifier})`
        );
        return channel.ack(message as ConsumeMessage);
      }

      await this.useCase.execute(jsonObject);

      channel.ack(message as ConsumeMessage);

      if (jsonObject?.requeueUid)
        await this.messagingPublisher.sendMessage({
          exchange: "success.messages",
          message: { requeueUid: jsonObject?.requeueUid },
        });

      this.logger.info(
        {
          queue: queueName,
          exchange: this.options.exchange,
          habitarProcessUid,
          identifier,
        },
        `${successMessage} (queue="${queueName}", ${identifier})`
      );
    } catch (error: any) {
      channel.nack(message as ConsumeMessage, undefined, this.config.requeue);

      const handledError = this.errorHandler.handleError(error, {
        exchange: this.options.exchange,
        message: jsonObject,
        queue: queueName,
        habitarProcessUid,
        identifier,
      });

      await this.messagingPublisher.sendMessage({
        exchange: "error.messages",
        message: {
          requeueUid: jsonObject?.requeueUid || v4(),
          origin: process.env.SERVICE_NAME,
          queue: queueName,
          data: jsonObject,
          error: {
            message: handledError.message,
            errors: handledError.errors,
            statusCode: handledError.statusCode,
            stack: handledError.stack,
          },
        },
      });
    }
  }
}
