type ExtractWordOrSymbol<S extends string> = S extends `(${infer Rest}`
  ? ['(', ...ExtractWordOrSymbol<Rest>]
  : S extends `)${infer Rest}`
    ? [')', ...ExtractWordOrSymbol<Rest>]
    : S extends `,${infer Rest}`
      ? [',', ...ExtractWordOrSymbol<Rest>]
      : S extends `"${infer Rest}`
        ? ['"', ...ExtractWordOrSymbol<Rest>]
        : S extends `${infer Start}(`
          ? [...ExtractWordOrSymbol<Start>, '(']
          : S extends `${infer Start})`
            ? [...ExtractWordOrSymbol<Start>, ')']
            : S extends `${infer Start},`
              ? [...ExtractWordOrSymbol<Start>, ',']
              : S extends `${infer Start}"`
                ? [...ExtractWordOrSymbol<Start>, '"']
                : S extends `${infer Start};`
                  ? [...ExtractWordOrSymbol<Start>, ';']
                  : S extends ''
                    ? []
                    : [S];
type SplitToWordsAndSymbols<S extends string> = S extends `${infer Word} ${infer Rest}`
  ? [...ExtractWordOrSymbol<Word>, ...SplitToWordsAndSymbols<Rest>]
  : ExtractWordOrSymbol<S>;
type TrimString<S extends string> = S extends ` ${infer Rest}` | `${infer Rest} `
  ? TrimString<Rest>
  : S;
export type TokenizeSqlString<S extends string> = S extends `${infer Line}\n${infer Rest}`
  ? [...SplitToWordsAndSymbols<TrimString<Line>>, ...TokenizeSqlString<Rest>]
  : SplitToWordsAndSymbols<TrimString<S>>;
export {};
