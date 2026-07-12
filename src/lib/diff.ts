// 基于 LCS 的按行 diff（无外部依赖），用于消息对比。
export type DiffOp = { type: "same" | "add" | "del"; text: string }

export function lineDiff(a: string, b: string): DiffOp[] {
  const al = a.split("\n")
  const bl = b.split("\n")
  const n = al.length
  const m = bl.length
  // LCS 长度表
  const dp: number[][] = Array.from({ length: n + 1 }, () => new Array(m + 1).fill(0))
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = al[i] === bl[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1])
    }
  }
  const ops: DiffOp[] = []
  let i = 0
  let j = 0
  while (i < n && j < m) {
    if (al[i] === bl[j]) {
      ops.push({ type: "same", text: al[i] })
      i++
      j++
    } else if (dp[i + 1][j] >= dp[i][j + 1]) {
      ops.push({ type: "del", text: al[i] })
      i++
    } else {
      ops.push({ type: "add", text: bl[j] })
      j++
    }
  }
  while (i < n) ops.push({ type: "del", text: al[i++] })
  while (j < m) ops.push({ type: "add", text: bl[j++] })
  return ops
}
