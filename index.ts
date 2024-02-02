import fs from "fs";
import csv from "csv-parser";
import { createWriteStream } from "fs";

interface FunctionCall {
  name: string;
  arguments: string;
}

interface AdditionalKwargs {
  tool_calls?: Array<{
    id: string;
    type: string;
    function: FunctionCall;
  }>;
}

interface Data {
  content: string;
  additional_kwargs?: AdditionalKwargs;
  tool_call_id?: string; // For messages that are function/tool calls
}

interface CsvMessage {
  data: Data;
  type: string; // 'system', 'ai', 'human', or 'tool'
}

interface Message {
  role: "system" | "assistant" | "user" | "function";
  content: string | null;
  function_call?: FunctionCall;
  name?: string; // For function/tool call identification
}

// Helper function to parse JSON safely
const parseJson = <T>(text: string): T | null => {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
};

// Function to transform a row from the CSV to the desired JSON structure
const transformRow = (input: string, output: string): Message[] => {
  const inputMessages = parseJson<CsvMessage[]>(input);
  const outputData = parseJson<{ data: Data; type: string }>(output)?.data;

  const messages: Message[] =
    inputMessages?.map((msg) => {
      let role: Message["role"] = "system"; // Default to 'system'
      switch (msg.type) {
        case "ai":
          role = "assistant";
          break;
        case "human":
          role = "user";
          break;
        case "tool":
          role = "function";
          break;
      }

      const baseMessage: Message = { role, content: msg.data.content };

      if (msg.type === "tool") {
        return { ...baseMessage, name: msg.data.tool_call_id }; // For tool/function messages
      } else if (msg.data.additional_kwargs?.tool_calls?.length) {
        const { name, arguments: args } =
          msg.data.additional_kwargs.tool_calls[0].function;
        return { ...baseMessage, function_call: { name, arguments: args } };
      }

      return baseMessage;
    }) || [];

  if (outputData) {
    const functionCall =
      outputData.additional_kwargs?.tool_calls?.[0]?.function;
    if (functionCall) {
      messages.push({
        role: "assistant",
        content: null,
        function_call: {
          name: functionCall.name,
          arguments: functionCall.arguments,
        },
      });
    }

    if (outputData.content) {
      messages.push({ role: "assistant", content: outputData.content });
    }
  }

  return messages;
};

// Main function to read the CSV and write to JSONL
const convertCSVtoJSONL = (sourceFile: string, outputFile: string) => {
  const readStream = fs.createReadStream(sourceFile);
  const writeStream = createWriteStream(outputFile);

  readStream
    .pipe(csv())
    .on("data", (row) => {
      const { input_input, output_output } = row;
      const messages = transformRow(input_input, output_output);
      writeStream.write(JSON.stringify({ messages }) + "\n");
    })
    .on("end", () => {
      console.log("CSV has been converted to JSONL.");
    });
};

convertCSVtoJSONL("source.csv", "output.jsonl");
