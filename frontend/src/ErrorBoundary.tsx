import { Component, ErrorInfo, ReactNode } from 'react'

type Props = { children: ReactNode }
type State = { hasError: boolean; message: string }

export class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, message: '' }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, message: error.message || '未知错误' }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // eslint-disable-next-line no-console
    console.error('Global UI error:', error, info)
  }

  render() {
    if (this.state.hasError) {
      return (
        <section className="panel error-panel">
          <h2>页面发生错误</h2>
          <p>{this.state.message}</p>
          <button onClick={() => window.location.assign('/')}>返回首页</button>
        </section>
      )
    }
    return this.props.children
  }
}
