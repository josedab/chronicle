/**
 * Chronicle WASM loader.
 *
 * Loads the Go WASM runtime and initializes the Chronicle database.
 *
 * @example
 * ```javascript
 * import { loadChronicle } from '@chronicle-db/wasm';
 * const chronicle = await loadChronicle();
 * await chronicle.open('data.db');
 * ```
 */

const WASM_URL = new URL('./chronicle.wasm', import.meta.url);

let wasmInstance = null;

/**
 * Load the Chronicle WASM module. Call once at startup.
 * @param {string} [wasmPath] - Optional custom path to the .wasm file.
 * @returns {Promise<typeof globalThis.chronicle>} The chronicle API object.
 */
export async function loadChronicle(wasmPath) {
  if (wasmInstance) {
    return globalThis.chronicle;
  }

  // Load wasm_exec.js Go runtime support
  if (!globalThis.Go) {
    throw new Error(
      'Go WASM runtime not loaded. Include wasm_exec.js before calling loadChronicle().'
    );
  }

  const go = new globalThis.Go();
  const url = wasmPath || WASM_URL;
  const result = await WebAssembly.instantiateStreaming(fetch(url), go.importObject);
  wasmInstance = result.instance;

  // Start the Go program (non-blocking)
  go.run(wasmInstance);

  // Wait for chronicle global to be available
  await new Promise((resolve) => {
    const check = () => {
      if (globalThis.chronicle) {
        resolve();
      } else {
        setTimeout(check, 10);
      }
    };
    check();
  });

  return globalThis.chronicle;
}

export default loadChronicle;
