// Shared RPC command and response type definitions

// Basic types
export type ExecuteOutput = string | number | boolean | null | undefined;

// Anonymize command and response
export type Anonymize = "Anonymize";
export type AnonymizeResponse =
  | "AnonymizeSuccess"
  | { AnonymizeFailure: string };

// WhoAmI command and response
export type WhoAmI = "WhoAmI";
export type WhoAmIResponse =
  | { Anonymous: { session_id: Uint8Array } }
  | { Identified: { session_id: Uint8Array; identity_id: Uint8Array } };

// Execute commands
export type ExecuteHash = { ExecuteHash: [string, string, any[]] };
export type Execute = {
  Execute: [string, string, any[]];
};

// Execute response types
export type ExecuteLog = {
  level: string;
  args: ExecuteOutput[];
};

export type ExecuteSuccess = {
  output: ExecuteOutput;
  duration: {
    secs: number;
    nanos: number;
  };
  logs: ExecuteLog[];
};

// Identify command and response
export type Identify = {
  Identify: [Uint8Array, Uint8Array];
};

export type IdentifyResponse = {
  IdentifySuccess: {
    session_id: string;
    identity: any;
  };
  IdentifyFailure: {
    error: string;
  };
};

// Union of all RPC commands
export type RpcCall = Anonymize | Execute | ExecuteHash | Identify | WhoAmI;
