import { ActorCommunicationProtocol } from "@digitalcreation/aws-lambda-actors/src/actorCommunicationProtocol"
import { Message } from "@digitalcreation/aws-lambda-actors/src/message"
import { ActorRef } from "@digitalcreation/aws-lambda-actors/src/actorRef";
import { Command } from "@digitalcreation/aws-lambda-actors/src/command";
import { SQSEvent } from "aws-lambda";
import { SQS } from 'aws-sdk';

const groupBy = <T>(arr: T[], getKey: (item: T) => string): { [key: string]: T[] } => {
    return arr.reduce((storage, item) => {
        const objKey = getKey(item);
        if (storage[objKey]) {
            storage[objKey].push(item);
        } else {
            storage[objKey] = [item];
        }
        return storage;
    }, {} as { [key: string]: T[] });
}

export class SqsCommunicationProtocol implements ActorCommunicationProtocol {
    private _sqs: SQS;
    private _queueUrl: string;

    constructor(region: string, accountId: string, queueName: string) {
        this._sqs = new SQS();
        this._queueUrl = `https://sqs.${region}.amazonaws.com/${accountId}/${queueName}`;
    }

    receive(input: any): { receiver: ActorRef; commands: Command[] }[] {
        const sqsInput = input as SQSEvent;

        if (!sqsInput) {
            return [];
        }

        const grouped = groupBy(sqsInput.Records, x => x.attributes.MessageGroupId || '');

        const result: { receiver: ActorRef; commands: Command[] }[] = [];

        for (const groupedKey in grouped) {
            const receiver = ActorRef.parse(groupedKey, this);

            if (!receiver) {
                continue;
            }

            result.push({
                receiver: receiver,
                commands: grouped[groupedKey].map(x => new Command(
                    x.messageAttributes["Type"].stringValue || '',
                    JSON.parse(x.body),
                    ActorRef.parse(x.messageAttributes["Sender"].stringValue || '', this)))
            })
        }

        return result;
    }

    async send(id: string, message: Message, sender: ActorRef): Promise<void> {
        await this._sqs.sendMessage({
            QueueUrl: this._queueUrl,
            MessageBody: JSON.stringify(message.body),
            MessageAttributes: {
                Sender: {
                    StringValue: sender.toString(),
                    DataType: 'String',
                },
                Type: {
                    StringValue: message.type,
                    DataType: 'String',
                },
            },
            MessageSystemAttributes: {
                MessageGroupId: {
                    StringValue: id,
                    DataType: 'String',
                }
            }
        }).promise();
    }
}
