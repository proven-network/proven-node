use deno_core::extension;

extension!(
    typescript_ext,
    esm_entry_point = "proven:typescript",
    esm = ["proven:typescript" = "vendor/typescript/index.mjs",],
);
