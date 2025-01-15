import { z } from "zod";

export const test = () => {
    const schema = z.object({
        name: z.string(),
        age: z.number(),
    });

    return schema.parse({ name: "Alice", age: 30 });
}
