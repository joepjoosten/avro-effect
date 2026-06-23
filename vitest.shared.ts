import type { UserConfig } from "vitest/config"

const config: UserConfig = {
  esbuild: {
    target: "es2022"
  },
  test: {
    include: ["test/**/*.test.ts"],
    exclude: ["**/build/**", "**/dist/**", "**/node_modules/**"],
    sequence: {
      concurrent: true
    }
  }
}

export default config
