import amqp, { Channel, Connection } from 'amqplib'

class MessageSender {
  private static instance: MessageSender
  private connection: Connection | null = null
  private channel: Channel | null = null

  private constructor() {}

  private async init() {
    this.connection = await amqp.connect(
      process.env.RABBITMQ_URL || 'amqp://localhost',
    )
    this.channel = await this.connection.createChannel()
  }

  static async getInstance() {
    if (!MessageSender.instance) {
      MessageSender.instance = new MessageSender()
      await MessageSender.instance.init()
    }

    return MessageSender.instance
  }

  async send(queue: string, message: Buffer) {
    const channel = this.channel

    if (!channel) {
      return
    }

    await channel.assertQueue(queue, { durable: true })

    channel.sendToQueue(queue, message)
  }
}

export default MessageSender
