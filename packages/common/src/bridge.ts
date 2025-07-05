// Message types for parent ↔ bridge communication

export type WhoAmIMessage = {
  type: 'whoAmI';
  nonce: number;
};

export type ExecuteMessage = {
  type: 'execute';
  nonce: number;
  data: {
    script: string;
    handler: string;
    args?: any[];
  };
};

export type ParentToBridgeMessage = WhoAmIMessage | ExecuteMessage;

export type ResponseMessage = {
  type: 'response';
  nonce: number;
  success: boolean;
  data?: any;
  error?: string;
};

export type OpenModalMessage = {
  type: 'open_registration_modal';
};

export type CloseModalMessage = {
  type: 'close_registration_modal';
};

export type BridgeToParentMessage = ResponseMessage | OpenModalMessage | CloseModalMessage;
