import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  try {
    const { awardBody, movieId } = event.pathParameters || {};

    // Validate input parameters
    if (!awardBody || !movieId) {
      return {
        statusCode: 400,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          message: "awardBody and movieId are required.",
        }),
      };
    }

    // Query DynamoDB for the awards
    const commandInput: QueryCommandInput = {
      TableName: process.env.TABLE_NAME, // Ensure TABLE_NAME is set in your environment variables
      IndexName: "awardBodyIx", // Replace with the actual index for `awardBody` if applicable
      KeyConditionExpression: "awardBody = :a AND movieId = :m",
      ExpressionAttributeValues: {
        ":a": awardBody,
        ":m": parseInt(movieId),
      },
    };

    const commandOutput = await ddbDocClient.send(new QueryCommand(commandInput));
    const awards = commandOutput.Items || [];

    if (awards.length === 0) {
      return {
        statusCode: 404,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          message: `No awards found for movieId ${movieId} and awardBody ${awardBody}.`,
        }),
      };
    }

    return {
      statusCode: 200,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ data: awards }),
    };
  } catch (error: any) {
    console.error("Error: ", error);
    return {
      statusCode: 500,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ error: error.message }),
    };
  }
};

function createDocumentClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = { wrapNumbers: false };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
