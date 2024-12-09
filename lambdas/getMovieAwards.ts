import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";


const ddbClient = new DynamoDBClient({ region: process.env.REGION });
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  try {
    const { awardBody, movieId } = event.pathParameters || {};
    const minAwards = parseInt(event.queryStringParameters?.min || "0");

    if (!awardBody || !movieId) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          message: "awardBody and movieId are required parameters.",
        }),
      };
    }

    const commandInput: QueryCommandInput = {
      TableName: process.env.TABLE_NAME,
      KeyConditionExpression: "movieId = :m AND awardBody = :a",
      ExpressionAttributeValues: {
        ":m": parseInt(movieId),
        ":a": awardBody,
      },
    };

    const commandOutput = await ddbDocClient.send(new QueryCommand(commandInput));
    const awards = commandOutput.Items || [];

    // Filter awards based on the minimum number of awards
    const filteredAwards = awards.filter(
      (award: any) => award.numAwards > minAwards
    );

    if (filteredAwards.length === 0) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          message: `Request failed: No awards found with more than ${minAwards} awards for movieId ${movieId} and awardBody ${awardBody}.`,
        }),
      };
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ data: filteredAwards }),
    };
  } catch (error: any) {
    console.error("Error: ", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};

