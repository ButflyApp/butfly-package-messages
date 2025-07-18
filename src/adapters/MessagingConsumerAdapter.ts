import { Channel, Connection, ConsumeMessage } from "amqplib";
import { Logger } from "winston";
import { IMessagingConsumerAdapter } from "../contracts/IMessagingConsumer";
import { IMessagingPublisherAdapter } from "../contracts/IMessagingPublisher";
import { v4 } from "uuid";
import { ErrorHandler } from "./ErrorHandler";

type RabbitConsumerAdapterOptions = {
  exchange: string;
  queue: string;
  successMessage: string;
};

export class RabbitConsumerAdapter implements IMessagingConsumerAdapter {
  private useCase: { execute: (data: any) => Promise<any> | any };
  private options: RabbitConsumerAdapterOptions;
  private messagingPublisher: IMessagingPublisherAdapter;
  private logger: Logger;
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
      logger: Logger;
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

    const { channel, connection } = await this.amqpConnection({
      exchange,
      queues: [queueName],
    });

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
    for (const item of queueMessage) {
      await channel.sendToQueue(queueName, Buffer.from(JSON.stringify(item)));
    }
    channel.ack(message as ConsumeMessage)
  }

  private async consumeObject(
    channel: Channel,
    jsonObject: any,
    message: ConsumeMessage | null,
    queueName: string,
    successMessage: string
  ) {
    try {
      if (jsonObject.messageOrigin === process.env.SERVICE_NAME)
        return channel.ack(message as ConsumeMessage);

      await this.useCase.execute(jsonObject);

      channel.ack(message as ConsumeMessage);

      if (jsonObject?.requeueUid)
        await this.messagingPublisher.sendMessage({
          exchange: "success.messages",
          message: { requeueUid: jsonObject?.requeueUid },
        });

      this.logger.info(`${successMessage}`, JSON.stringify(jsonObject));
    } catch (error: any) {
      channel.nack(message as ConsumeMessage, undefined, this.config.requeue);

      const handledError = this.errorHandler.handleError(error, {
        exchange: this.options.exchange,
        message: jsonObject,
        queue: queueName,
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
