export interface IMessagingPublisherAdapter {
  sendMessage({ exchange: string, message: any }): Promise<void>
}
