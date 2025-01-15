import { RadixEngineToolkit } from "@radixdlt/radix-engine-toolkit";

export const test = async () => {
    return JSON.stringify(await RadixEngineToolkit.Build.information());
}
