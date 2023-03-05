import { DynamoDBClient, DeleteItemCommand, GetItemCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { S3Client, DeleteObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { readFile } from "fs/promises";
import { CloudAdapter } from "..";
import { throws } from "../../../errors";
import { Project } from "../../../project";

export function awsCloudAdapter(project: Project): CloudAdapter {
  const { aws: { region, table, bucket } } = project;

  const ddb = new DynamoDBClient({
    region,
  });
  const s3 = new S3Client({
    region,
  });

  return {
    // dynamo
    delete: async (key) => {
      await ddb.send(new DeleteItemCommand({
        Key: marshall({ key }),
        TableName: table,
      }));
    },
    load: async (key) => {
      return await ddb.send(new GetItemCommand({
        Key: marshall({ key }),
        TableName: table,
      })).then(x => x.Item === undefined ? throws(`${key} not found`) : unmarshall(x.Item));
    },
    save: async (key, value) => {
      await ddb.send(new PutItemCommand({
        Item: marshall({ key, ...value }),
        TableName: table,
      }));
    },
    // s3
    remove: async (path) => {
      await s3.send(new DeleteObjectCommand({
        Bucket: bucket,
        Key: path,
      }));
    },
    upload: async (from, to) => {
      await s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: to,
        Body: await readFile(from)
      }));
    },
  };
}
