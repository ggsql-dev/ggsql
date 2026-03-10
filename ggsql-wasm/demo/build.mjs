import * as esbuild from "esbuild";
import { copyFileSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const isWatch = process.argv.includes("--watch");
const distDir = join(__dirname, "dist");

// Ensure dist/ directory exists
mkdirSync(distDir, { recursive: true });

// Copy static files
console.log("Copying static files...");
copyFileSync(join(__dirname, "src/index.html"), join(distDir, "index.html"));
copyFileSync(
  join(__dirname, "../pkg/ggsql_wasm_bg.wasm"),
  join(distDir, "ggsql_wasm_bg.wasm"),
);
copyFileSync(
  join(__dirname, "node_modules/vscode-oniguruma/release/onig.wasm"),
  join(distDir, "onig.wasm"),
);
copyFileSync(
  join(__dirname, "../../ggsql-vscode/syntaxes/ggsql.tmLanguage.json"),
  join(distDir, "ggsql.tmLanguage.json"),
);

// Build Monaco editor web worker
console.log("Building Monaco editor worker...");
await esbuild.build({
  entryPoints: [
    join(
      __dirname,
      "node_modules/monaco-editor/esm/vs/editor/editor.worker.js",
    ),
  ],
  bundle: true,
  outfile: join(distDir, "editor.worker.js"),
  format: "iife",
});

// Build main application bundle
const buildOptions = {
  entryPoints: [join(__dirname, "src/main.ts")],
  bundle: true,
  outfile: join(distDir, "bundle.js"),
  format: "esm",
  platform: "browser",
  target: "es2020",
  sourcemap: true,
  nodePaths: [join(__dirname, "node_modules")],
  loader: {
    ".ttf": "file",
  },
};

if (isWatch) {
  console.log("Starting watch mode...");
  const ctx = await esbuild.context(buildOptions);
  await ctx.watch();
  console.log("Watching for changes...");
} else {
  console.log("Building main bundle...");
  await esbuild.build(buildOptions);
  console.log("Build complete!");
}
