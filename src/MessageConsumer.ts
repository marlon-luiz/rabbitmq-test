import amqp, { Channel, Connection, Message } from 'amqplib'

type Handler = (msg: Message) => void

class MessageConsumer {
  private static instance: MessageConsumer
  private connection: Connection | null = null
  private channel: Channel | null = null
  private queues: { [key: string]: Handler[] } = {}

  private constructor() {}

  private async init() {
    this.connection = await amqp.connect(
      process.env.RABBITMQ_URL || 'amqp://localhost',
    )
    this.channel = await this.connection.createChannel()
  }

  static async getInstance() {
    if (!MessageConsumer.instance) {
      MessageConsumer.instance = new MessageConsumer()
      await MessageConsumer.instance.init()
    }

    return MessageConsumer.instance
  }

  async subscribe(queue: string, handler: Handler) {
    const channel = this.channel

    if (!channel) {
      return () => this.unsubscribe(queue, handler)
    }

    if (this.queues[queue]) {
      const existingHandler = this.queues[queue].find(
        queueHandle => queueHandle === handler,
      )

      if (existingHandler) {
        return () => this.unsubscribe(queue, existingHandler)
      }

      this.queues[queue].push(handler)
      return () => this.unsubscribe(queue, handler)
    }

    await channel.assertQueue(queue, { durable: true })

    this.queues[queue] = [handler]

    channel.consume(queue, msg => {
      if (msg === null) {
        return
      }

      channel.ack(msg)

      this.queues[queue].forEach(h => h(msg))
    })

    return () => this.unsubscribe(queue, handler)
  }

  private async unsubscribe(queue: string, handler: Handler) {
    this.queues[queue] = this.queues[queue].filter(h => h !== handler)
  }
}

export default MessageConsumer
