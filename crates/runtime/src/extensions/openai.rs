use deno_core::extension;

extension!(
  openai_ext,
  esm_entry_point = "proven:openai",
  esm = [dir "vendor/openai",
      "proven:openai" = "index.mjs"
  ],
);
