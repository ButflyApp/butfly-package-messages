export interface IMessagingPublisherAdapter {
  sendMessage({ exchange, queue, message, config }: { exchange?: string, queue?: string, message: any, config?: { autoSetOrigin?: boolean } }): Promise<void>
}
