import React from "react"
import ReactDOM from "react-dom/client"
import App from "./App"
import "./index.css"

// 禁用浏览器默认右键菜单（桌面应用风格）
document.addEventListener("contextmenu", (e) => e.preventDefault())

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)
