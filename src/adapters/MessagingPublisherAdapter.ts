import { connect } from "amqplib";
import { Logger } from "winston";
import { IMessagingPublisherAdapter } from "../contracts/IMessagingPublisher";
import { v4 as uuidv4 } from 'uuid';

export class RabbitPublisherAdapter implements IMessagingPublisherAdapter {
  private logger: Logger
  private defaultConfig = { autoSetOrigin: true }
  constructor({ logger }: { logger: Logger }) {
    this.logger = logger
  }

  async sendMessage({ exchange, queue, message, config = this.defaultConfig }: { exchange?: string; queue?: string; message: any, config?: { autoSetOrigin?: boolean } }) {
    if (config.autoSetOrigin) message.messageOrigin = process.env.SERVICE_NAME
    if(!message.habitarProcessUid) message.habitarProcessUid = uuidv4()
    const connection = await connect({
      heartbeat: 5,
      hostname: process.env.AMQP_CONNECTION,
      username: process.env.AMQP_USER,
      password: process.env.AMQP_PASS,
    })
    const channel = await connection.createChannel()

    if (exchange) {
      await channel.assertExchange(exchange, "fanout")
      this.logger.info(`+ New message on exchange "${exchange}"`)
      channel.publish(exchange, "", Buffer.from(JSON.stringify(message)))
    } else if (queue) {
      this.logger.info(`+ New message on queue "${exchange}"`)
      channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)))
    }

    await channel.close()
    await connection.close()
  }
}
