import express from 'express'

import MessageConsumer from './MessageConsumer'
import MessageSender from './MessageSender'

const app = express()
app.use(express.json())

app.post('/queue', async (req, res) => {
  const sender = await MessageSender.getInstance()

  sender.send('test-queue', Buffer.from(JSON.stringify(req.body)))

  return res.json({ message: 'Enviado para a fila' })
})

app.listen(3000, () => console.log('Server running on port 3000...'))

MessageConsumer.getInstance().then(consumer => {
  consumer.subscribe('test-queue', msg => {
    console.log('subscribe test-queue', JSON.parse(msg.content.toString()))
  })
})
