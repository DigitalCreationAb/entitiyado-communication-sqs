import { CommunicationProtocol, EntityRef, Command } from "@entitiyado/core"
import { SQSEvent } from "aws-lambda";
import { SQS } from 'aws-sdk';
import { Md5 } from 'ts-md5/dist/md5';

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

export class SqsCommunicationProtocol implements CommunicationProtocol {
    private readonly _sqs: SQS;
    private readonly _region: string;
    private readonly _accountId: string;

    constructor(region: string, accountId: string) {
        this._sqs = new SQS();
        this._region = region;
        this._accountId = accountId;
    }

    receive(input: any): { receiver: EntityRef; commands: Command[] }[] {
        const sqsInput = input as SQSEvent;

        if (!sqsInput) {
            return [];
        }

        const grouped = groupBy(sqsInput.Records, x => x.messageAttributes.Receiver.stringValue || '');

        const result: { receiver: EntityRef; commands: Command[] }[] = [];

        for (const groupedKey in grouped) {
            if (!(groupedKey in grouped)) {
                continue;
            }

            const receiver = EntityRef.parse(groupedKey, this);

            if (!receiver) {
                continue;
            }

            result.push({
                receiver,
                commands: grouped[groupedKey].map(x => new Command(
                    x.messageAttributes.Type.stringValue || '',
                    JSON.parse(x.body),
                    EntityRef.parse(x.messageAttributes.Sender.stringValue || '', this)))
            });
        }

        return result;
    }

    async send(to: EntityRef, command: Command): Promise<void> {
        const queueUrl = `https://sqs.${this._region}.amazonaws.com/${this._accountId}/${to.type}`;
        const groupId = Md5.hashStr(to.toString()) as string;

        await this._sqs.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: JSON.stringify(command.body),
            MessageAttributes: {
                Receiver: {
                    StringValue: to.toString(),
                    DataType: 'String',
                },
                Sender: {
                    StringValue: command.sender?.toString() || '',
                    DataType: 'String',
                },
                Type: {
                    StringValue: command.type,
                    DataType: 'String',
                },
            },
            MessageGroupId: groupId
        }).promise();
    }
}
