import { FormEvent, useEffect, useMemo, useState } from 'react'
import { Link, Navigate, Route, Routes, useLocation, useNavigate, useParams } from 'react-router-dom'
import { api, CartItem, OrderDetail, OrderSummary, Product, setUnauthorizedHandler } from './api'

const IDEM_STORAGE_KEY = 'checkout_retry_idem_key'
const REPLAY_STORAGE_PREFIX = 'order_replay_'
const PAGE_SIZE_STORAGE_KEY = 'orders_page_size'
const INCLUDE_TOTAL_STORAGE_KEY = 'orders_include_total'
const ORDER_FILTERS_STORAGE_KEY = 'orders_filter_state'

const ORDER_STATUS_OPTIONS = [
  { value: '', label: '全部状态' },
  { value: 'created', label: '已创建' },
  { value: 'processing', label: '处理中' },
  { value: 'shipped', label: '已发货' },
  { value: 'completed', label: '已完成' },
  { value: 'cancelled', label: '已取消' },
  { value: 'failed', label: '失败' }
]
const ORDER_STATUS_VALUES = ORDER_STATUS_OPTIONS.filter((opt) => opt.value).map((opt) => opt.value)
const ORDER_STATUS_LABELS = new Map(ORDER_STATUS_OPTIONS.filter((opt) => opt.value).map((opt) => [opt.value, opt.label]))

const PAGE_SIZE_OPTIONS = [10, 20, 50, 100]
const MAX_ORDER_ID_FILTERS = 50

const ORDER_STATUS_META: Record<string, { label: string; tone: string }> = {
  created: { label: '已创建', tone: 'info' },
  processing: { label: '处理中', tone: 'warning' },
  shipped: { label: '已发货', tone: 'success' },
  completed: { label: '已完成', tone: 'success' },
  cancelled: { label: '已取消', tone: 'neutral' },
  failed: { label: '失败', tone: 'danger' }
}

const formatDateTime = (value?: string) => {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

const escapeCSV = (value: string | number | undefined | null) => {
  const text = value === undefined || value === null ? '' : String(value)
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`
  }
  return text
}

const padTime = (value: number) => value.toString().padStart(2, '0')

const formatDateTimeInput = (date: Date) => (
  `${date.getFullYear()}-${padTime(date.getMonth() + 1)}-${padTime(date.getDate())}T${padTime(date.getHours())}:${padTime(date.getMinutes())}`
)

const parseStatusFilters = (value: string) => {
  if (!value) return []
  const tokens = value
    .split(/[\s,]+/)
    .map((token) => token.trim().toLowerCase())
    .filter(Boolean)
  if (tokens.length === 0) return []
  const set = new Set(tokens)
  return ORDER_STATUS_VALUES.filter((status) => set.has(status))
}

const normalizeStatusFilters = (value: string) => parseStatusFilters(value).join(',')

const parseAmountFilter = (raw: string) => {
  const trimmed = raw.trim()
  if (!trimmed) return { valid: true, value: null as number | null }
  if (!/^\d*(\.\d*)?$/.test(trimmed) || trimmed === '.') {
    return { valid: false, value: null as number | null }
  }
  const parsed = Number(trimmed)
  if (!Number.isFinite(parsed) || parsed < 0) return { valid: false, value: null as number | null }
  return { valid: true, value: parsed }
}

const loadOrderFilters = () => {
  const raw = localStorage.getItem(ORDER_FILTERS_STORAGE_KEY)
  if (!raw) return { status: '', from: '', to: '', orderQuery: '', minAmount: '', maxAmount: '' }
  try {
    const parsed = JSON.parse(raw) as {
      status?: string
      from?: string
      to?: string
      orderQuery?: string
      minAmount?: string
      maxAmount?: string
    }
    return {
      status: typeof parsed.status === 'string' ? normalizeStatusFilters(parsed.status) : '',
      from: typeof parsed.from === 'string' ? parsed.from : '',
      to: typeof parsed.to === 'string' ? parsed.to : '',
      orderQuery: typeof parsed.orderQuery === 'string' ? parsed.orderQuery : '',
      minAmount: typeof parsed.minAmount === 'string' ? parsed.minAmount : '',
      maxAmount: typeof parsed.maxAmount === 'string' ? parsed.maxAmount : ''
    }
  } catch {
    return { status: '', from: '', to: '', orderQuery: '', minAmount: '', maxAmount: '' }
  }
}

const persistOrderFilters = (filters: {
  status: string
  from: string
  to: string
  orderQuery: string
  minAmount: string
  maxAmount: string
}) => {
  const normalized = {
    status: normalizeStatusFilters(filters.status || ''),
    from: filters.from || '',
    to: filters.to || '',
    orderQuery: filters.orderQuery || '',
    minAmount: filters.minAmount || '',
    maxAmount: filters.maxAmount || ''
  }
  if (!normalized.status && !normalized.from && !normalized.to && !normalized.orderQuery && !normalized.minAmount && !normalized.maxAmount) {
    localStorage.removeItem(ORDER_FILTERS_STORAGE_KEY)
    return
  }
  localStorage.setItem(ORDER_FILTERS_STORAGE_KEY, JSON.stringify(normalized))
}

const copyTextToClipboard = async (text: string) => {
  if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text)
    return true
  }
  const textarea = document.createElement('textarea')
  textarea.value = text
  textarea.setAttribute('readonly', '')
  textarea.style.position = 'absolute'
  textarea.style.left = '-9999px'
  document.body.appendChild(textarea)
  textarea.select()
  const success = document.execCommand('copy')
  document.body.removeChild(textarea)
  return success
}

function StatusTag({ status }: { status: string }) {
  const normalized = (status || '').trim().toLowerCase()
  const meta = ORDER_STATUS_META[normalized]
  const label = meta?.label || (normalized ? status.trim() : '未知')
  const tone = meta?.tone || 'neutral'
  const showCode = Boolean(normalized && label.toLowerCase() !== normalized)

  return (
    <span className="status-stack">
      <span className={`status-tag status-${tone}`}>{label}</span>
      {showCode && <span className="status-code">{status}</span>}
    </span>
  )
}

function TopNav({ cartCount, token }: { cartCount: number; token: string }) {
  return (
    <header className="hero">
      <div>
        <h1>NovaMart</h1>
        <p>Centralized Commerce Platform</p>
      </div>
      <nav>
        <Link to="/">首页</Link>
        <Link to="/cart">购物车({cartCount})</Link>
        <Link to="/orders">订单历史</Link>
        <Link to="/login">{token ? '账户' : '登录'}</Link>
      </nav>
    </header>
  )
}

function Loading({ show }: { show: boolean }) {
  return show ? <div className="banner">加载中...</div> : null
}

function OrderDetailPage({ token }: { token: string }) {
  const { orderId } = useParams()
  const location = useLocation()
  const [detail, setDetail] = useState<OrderDetail | null>(null)
  const [error, setError] = useState('')
  const [copyNotice, setCopyNotice] = useState('')
  const [copiedDetail, setCopiedDetail] = useState(false)
  const [copiedAddress, setCopiedAddress] = useState(false)
  const replayNotice = useMemo(() => {
    if (!orderId) return false
    const stateReplay = (location.state as { idempotentReplay?: boolean } | null)?.idempotentReplay
    if (stateReplay) return true
    return sessionStorage.getItem(`${REPLAY_STORAGE_PREFIX}${orderId}`) === 'true'
  }, [location.state, orderId])

  useEffect(() => {
    if (!token || !orderId) return
    api.getOrder(token, Number(orderId)).then(setDetail).catch((e) => setError(e.message))
  }, [token, orderId])

  useEffect(() => {
    if (!copyNotice) return
    const timer = window.setTimeout(() => setCopyNotice(''), 2000)
    return () => window.clearTimeout(timer)
  }, [copyNotice])

  useEffect(() => {
    if (!copiedDetail) return
    const timer = window.setTimeout(() => setCopiedDetail(false), 2000)
    return () => window.clearTimeout(timer)
  }, [copiedDetail])

  useEffect(() => {
    if (!copiedAddress) return
    const timer = window.setTimeout(() => setCopiedAddress(false), 2000)
    return () => window.clearTimeout(timer)
  }, [copiedAddress])

  async function handleCopyOrderId() {
    if (!detail) return
    try {
      const ok = await copyTextToClipboard(String(detail.order_id))
      if (ok) {
        setCopiedDetail(true)
        setCopyNotice(`已复制订单号 ${detail.order_id}`)
      } else {
        setCopiedDetail(false)
        setCopyNotice('复制失败，请手动选择订单号')
      }
    } catch {
      setCopiedDetail(false)
      setCopyNotice('复制失败，请手动选择订单号')
    }
  }

  async function handleCopyAddress() {
    if (!detail) return
    try {
      const ok = await copyTextToClipboard(detail.address)
      if (ok) {
        setCopiedAddress(true)
        setCopyNotice('已复制收货地址')
      } else {
        setCopiedAddress(false)
        setCopyNotice('复制失败，请手动选择收货地址')
      }
    } catch {
      setCopiedAddress(false)
      setCopyNotice('复制失败，请手动选择收货地址')
    }
  }

  if (!token) return <Navigate to="/login" replace />
  if (error) return <section className="panel error-panel"><h2>订单加载失败</h2><p>{error}</p></section>
  if (!detail) return <section className="panel"><p>订单加载中...</p></section>
  const hasIdemInfo = Boolean(detail.idempotency_key || detail.idempotency_created_at || detail.idempotency_last_replay_at)
  const itemsTotal = detail.items.reduce((sum, it) => sum + (it.subtotal ?? it.price * it.quantity), 0)
  const itemsCount = detail.items.reduce((sum, it) => sum + it.quantity, 0)
  const detailItemCount = typeof detail.item_count === 'number' ? detail.item_count : itemsCount
  const hasTotalGap = Math.abs(itemsTotal - detail.amount) > 0.01

  return (
    <section className="panel success">
      <h2>订单详情</h2>
      <div className="order-actions">
        <Link className="back-link" to="/orders">返回订单历史</Link>
        <Link className="back-link" to="/">继续购物</Link>
      </div>
      {copyNotice && <div className="copy-toast">{copyNotice}</div>}
      {replayNotice && (
        <div className="banner">该订单由幂等重放返回，重复提交不会再次扣款。</div>
      )}
      <div className="detail-card">
        <div className="detail-grid">
          <div className="detail-item">
            <span className="meta-label">订单号</span>
            <div className="detail-row">
              <span>{detail.order_id}</span>
              <button
                className={`ghost-btn small-btn ${copiedDetail ? 'copy-success' : ''}`}
                type="button"
                onClick={handleCopyOrderId}
              >
                {copiedDetail ? '已复制' : '复制'}
              </button>
            </div>
          </div>
          <div className="detail-item">
            <span className="meta-label">状态</span>
            <StatusTag status={detail.status} />
          </div>
          <div className="detail-item">
            <span className="meta-label">下单时间</span>
            <span>{formatDateTime(detail.created_at)}</span>
          </div>
          <div className="detail-item">
            <span className="meta-label">金额</span>
            <span>¥{detail.amount.toFixed(2)}</span>
          </div>
          <div className="detail-item">
            <span className="meta-label">商品件数</span>
            <span>{detailItemCount} 件</span>
          </div>
          <div className="detail-item detail-address">
            <span className="meta-label">收货地址</span>
            <div className="detail-row">
              <span>{detail.address}</span>
              <button
                className={`ghost-btn small-btn ${copiedAddress ? 'copy-success' : ''}`}
                type="button"
                onClick={handleCopyAddress}
              >
                {copiedAddress ? '已复制' : '复制地址'}
              </button>
            </div>
          </div>
        </div>
      </div>
      {hasIdemInfo && (
        <div className="meta-card">
          <h3>幂等信息</h3>
          <div className="meta-grid">
            <div>
              <span className="meta-label">幂等键</span>
              <span>{detail.idempotency_key || '—'}</span>
            </div>
            <div>
              <span className="meta-label">首次创建</span>
              <span>{formatDateTime(detail.idempotency_created_at)}</span>
            </div>
            <div>
              <span className="meta-label">最近重放</span>
              <span>{formatDateTime(detail.idempotency_last_replay_at)}</span>
            </div>
          </div>
        </div>
      )}
      <div className="order-items">
        <div className="cart-row cart-row-head">
          <span>商品</span>
          <span>数量</span>
          <span>单价</span>
          <span>小计</span>
        </div>
        {detail.items.map((it) => {
          const subtotal = it.subtotal ?? it.price * it.quantity
          return (
          <div key={`${it.product_id}-${it.title}`} className="cart-row">
            <span>{it.title}</span>
            <span>x{it.quantity}</span>
            <span>¥{it.price.toFixed(2)}</span>
            <span>¥{subtotal.toFixed(2)}</span>
          </div>
          )
        })}
        <div className="cart-row cart-row-summary">
          <span>商品小计</span>
          <span>x{itemsCount}</span>
          <span />
          <span>¥{itemsTotal.toFixed(2)}</span>
        </div>
        {hasTotalGap && <small className="hint">订单金额已包含优惠或运费调整，以订单金额为准。</small>}
      </div>
    </section>
  )
}

function OrdersPage({ token }: { token: string }) {
  const [orders, setOrders] = useState<OrderSummary[]>([])
  const [storedFilters] = useState(loadOrderFilters)
  const [status, setStatus] = useState(storedFilters.status)
  const [from, setFrom] = useState(storedFilters.from)
  const [to, setTo] = useState(storedFilters.to)
  const [orderQuery, setOrderQuery] = useState(storedFilters.orderQuery)
  const [minAmount, setMinAmount] = useState(storedFilters.minAmount)
  const [maxAmount, setMaxAmount] = useState(storedFilters.maxAmount)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(() => {
    const raw = localStorage.getItem(PAGE_SIZE_STORAGE_KEY)
    const parsed = Number(raw)
    if (PAGE_SIZE_OPTIONS.includes(parsed)) return parsed
    return PAGE_SIZE_OPTIONS[0]
  })
  const [includeTotal, setIncludeTotal] = useState(() => {
    const raw = localStorage.getItem(INCLUDE_TOTAL_STORAGE_KEY)
    if (raw === 'false') return false
    if (raw === 'true') return true
    return true
  })
  const [total, setTotal] = useState(-1)
  const [cursor, setCursor] = useState('')
  const [nextCursor, setNextCursor] = useState('')
  const [cursorHistory, setCursorHistory] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [lastUpdated, setLastUpdated] = useState('')
  const [exportNotice, setExportNotice] = useState('')
  const [copyNotice, setCopyNotice] = useState('')
  const [copiedOrderId, setCopiedOrderId] = useState<number | null>(null)
  const statusTokens = useMemo(() => parseStatusFilters(status), [status])
  const statusHint = statusTokens.length
    ? `已选 ${statusTokens.length} 个状态，可继续叠加`
    : '默认展示全部状态，可多选叠加'
  const minAmountState = useMemo(() => parseAmountFilter(minAmount), [minAmount])
  const maxAmountState = useMemo(() => parseAmountFilter(maxAmount), [maxAmount])
  const minAmountValue = minAmountState.value
  const maxAmountValue = maxAmountState.value
  const minAmountInvalid = !minAmountState.valid
  const maxAmountInvalid = !maxAmountState.valid
  const amountInputInvalid = minAmountInvalid || maxAmountInvalid
  const amountRangeInvalid = !amountInputInvalid
    && minAmountValue !== null
    && maxAmountValue !== null
    && minAmountValue > maxAmountValue
  const minAmountHasError = minAmountInvalid || amountRangeInvalid
  const maxAmountHasError = maxAmountInvalid || amountRangeInvalid
  const normalizedQuery = orderQuery.trim()
  const queryTokens = useMemo(() => {
    if (!normalizedQuery) return []
    return normalizedQuery
      .split(/[\s,]+/)
      .map((token) => token.trim())
      .filter(Boolean)
  }, [normalizedQuery])
  const parsedOrderIds = useMemo(() => {
    const ids: number[] = []
    const invalid: string[] = []
    const seen = new Set<number>()
    for (const token of queryTokens) {
      if (!/^\d+$/.test(token)) {
        invalid.push(token)
        continue
      }
      const value = Number(token)
      if (!Number.isSafeInteger(value) || value <= 0) {
        invalid.push(token)
        continue
      }
      if (!seen.has(value)) {
        seen.add(value)
        ids.push(value)
      }
    }
    return { ids, invalid }
  }, [queryTokens])
  const orderIdFilters = parsedOrderIds.ids
  const invalidOrderTokens = parsedOrderIds.invalid
  const hasSearchInput = normalizedQuery.length > 0
  const hasSearch = orderIdFilters.length > 0
  const orderQueryHasInvalid = invalidOrderTokens.length > 0
  const orderQueryEmpty = hasSearchInput && orderIdFilters.length === 0
  const orderQueryTooMany = orderIdFilters.length > MAX_ORDER_ID_FILTERS
  const statusLabel = statusTokens.length > 0
    ? statusTokens.map((value) => ORDER_STATUS_LABELS.get(value) || value).join('、')
    : '全部状态'
  const rangeLabel = from || to
    ? `${from ? formatDateTime(from) : '不限开始'} 至 ${to ? formatDateTime(to) : '不限结束'}`
    : '全部时间'
  const amountLabel = amountInputInvalid
    ? '金额筛选无效'
    : minAmountValue === null && maxAmountValue === null
      ? '全部金额'
      : `金额 ${minAmountValue !== null ? `≥¥${minAmountValue}` : '不限下限'} · ${maxAmountValue !== null ? `≤¥${maxAmountValue}` : '不限上限'}`
  const rangeInvalid = Boolean(from && to && new Date(from) > new Date(to))
  const showClearFilters = Boolean(statusTokens.length || from || to || minAmount || maxAmount)
  const showClearSearch = Boolean(normalizedQuery)
  const hasActiveFilters = showClearFilters || showClearSearch
  const cursorMode = Boolean(cursor)
  const canResetPaging = page > 1 || cursorHistory.length > 0 || cursorMode
  const effectiveIncludeTotal = includeTotal && !cursorMode
  const totalAvailable = total >= 0
  const totalLabel = totalAvailable ? total : '—'
  const totalPages = totalAvailable ? Math.max(1, Math.ceil(total / pageSize)) : null
  const visibleOrders = orders
  const pageAmount = useMemo(
    () => visibleOrders.reduce((sum, order) => sum + order.amount, 0),
    [visibleOrders]
  )
  const averageAmount = visibleOrders.length > 0 ? pageAmount / visibleOrders.length : 0
  const averageAmountLabel = visibleOrders.length > 0 ? `¥${averageAmount.toFixed(2)}` : '—'
  const lastUpdatedLabel = lastUpdated ? formatDateTime(lastUpdated) : '未刷新'
  const pageCountLabel = hasSearchInput ? '匹配订单数' : '本页订单数'
  const pageAmountLabel = hasSearchInput ? '匹配金额合计' : '本页金额合计'
  const searchSummary = hasSearch
    ? orderIdFilters.length === 1
      ? `订单号为 ${orderIdFilters[0]}`
      : orderIdFilters.length <= 5
        ? `订单号为 ${orderIdFilters.join('、')}`
        : `订单号 ${orderIdFilters.slice(0, 5).join('、')} 等 ${orderIdFilters.length} 个`
    : ''
  const filterPieces = [statusLabel, rangeLabel, amountLabel]
  if (hasSearch) filterPieces.push(searchSummary)
  if (orderQueryHasInvalid) filterPieces.push('含无效订单号')
  const totalHint = cursorMode
    ? '游标翻页时不统计总数，返回首页可查看'
    : !includeTotal && totalAvailable
      ? '已使用缓存总数，开启可强制刷新'
      : '关闭可提升加载速度'
  const exportDisabled = loading
    || rangeInvalid
    || amountInputInvalid
    || amountRangeInvalid
    || orderQueryTooMany
    || visibleOrders.length === 0
  const exportHint = rangeInvalid
    ? '时间范围不合法，无法导出。'
    : amountInputInvalid
      ? '金额格式不正确，无法导出。'
    : amountRangeInvalid
      ? '金额范围不合法，无法导出。'
    : orderQueryTooMany
      ? `订单号最多支持 ${MAX_ORDER_ID_FILTERS} 个，请精简后再导出。`
    : orderQueryEmpty
      ? '订单号无有效数字，无法导出。'
    : visibleOrders.length === 0
      ? '当前没有可导出的订单。'
      : hasSearch
        ? '仅导出当前页匹配的订单。'
        : '仅导出当前页订单。'
  const exportLabel = exportNotice || '导出当前列表为 CSV（含地址）'

  function resetPaging() {
    setPage(1)
    setCursor('')
    setCursorHistory([])
    setNextCursor('')
  }

  function applyQuickRange(days: number) {
    const now = new Date()
    const start = new Date(now)
    start.setDate(now.getDate() - days)
    setFrom(formatDateTimeInput(start))
    setTo(formatDateTimeInput(now))
    resetPaging()
  }

  function applyCurrentMonth() {
    const now = new Date()
    const start = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0)
    setFrom(formatDateTimeInput(start))
    setTo(formatDateTimeInput(now))
    resetPaging()
  }

  function toggleStatusFilter(value: string) {
    if (!value) {
      setStatus('')
      resetPaging()
      return
    }
    const nextSet = new Set(statusTokens)
    if (nextSet.has(value)) {
      nextSet.delete(value)
    } else {
      nextSet.add(value)
    }
    const next = ORDER_STATUS_VALUES.filter((statusValue) => nextSet.has(statusValue))
    setStatus(next.join(','))
    resetPaging()
  }

  function clearFilters() {
    setStatus('')
    setFrom('')
    setTo('')
    setMinAmount('')
    setMaxAmount('')
    resetPaging()
  }

  function clearSearch() {
    setOrderQuery('')
    resetPaging()
  }

  function clearAllFilters() {
    clearFilters()
    clearSearch()
    setError('')
  }

  async function fetchOrders() {
    if (!token) return
    if (rangeInvalid || amountInputInvalid || amountRangeInvalid || orderQueryEmpty || orderQueryTooMany) {
      setOrders([])
      setTotal(0)
      setNextCursor('')
      setError('')
      return
    }
    setLoading(true)
    setError('')
    try {
      const data = await api.listOrders(token, {
        status: status || undefined,
        orderIds: orderIdFilters.length > 0 ? orderIdFilters : undefined,
        minAmount: minAmountValue ?? undefined,
        maxAmount: maxAmountValue ?? undefined,
        from: from ? new Date(from).toISOString() : undefined,
        to: to ? new Date(to).toISOString() : undefined,
        page,
        pageSize,
        cursor: cursor || undefined,
        includeTotal: effectiveIncludeTotal
      })
      setOrders(data.items)
      setTotal(data.total)
      setNextCursor(data.next_cursor || '')
      setLastUpdated(new Date().toISOString())
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchOrders().catch(() => undefined)
  }, [token, status, from, to, minAmount, maxAmount, page, pageSize, cursor, includeTotal, normalizedQuery])

  useEffect(() => {
    setExportNotice('')
  }, [status, from, to, minAmount, maxAmount, orderQuery, page, pageSize, cursor])

  useEffect(() => {
    persistOrderFilters({ status, from, to, orderQuery, minAmount, maxAmount })
  }, [status, from, to, orderQuery, minAmount, maxAmount])

  useEffect(() => {
    if (!copyNotice) return
    const timer = window.setTimeout(() => setCopyNotice(''), 2000)
    return () => window.clearTimeout(timer)
  }, [copyNotice])

  useEffect(() => {
    if (copiedOrderId === null) return
    const timer = window.setTimeout(() => setCopiedOrderId(null), 2000)
    return () => window.clearTimeout(timer)
  }, [copiedOrderId])

  function handleRefresh() {
    if (loading || rangeInvalid || amountInputInvalid || amountRangeInvalid) return
    fetchOrders().catch(() => undefined)
  }

  function exportOrdersCSV() {
    if (exportDisabled) {
      if (rangeInvalid) {
        setExportNotice('时间范围不合法')
      } else if (amountInputInvalid) {
        setExportNotice('金额格式不正确')
      } else if (amountRangeInvalid) {
        setExportNotice('金额范围不合法')
      } else if (orderQueryTooMany) {
        setExportNotice(`订单号最多支持 ${MAX_ORDER_ID_FILTERS} 个，请精简后再导出。`)
      } else if (orderQueryEmpty) {
        setExportNotice('订单号无有效数字，无法导出。')
      } else if (visibleOrders.length === 0) {
        setExportNotice('暂无可导出订单')
      } else if (loading) {
        setExportNotice('加载中，稍后重试')
      }
      return
    }
    const rows = [
      ['order_id', 'status', 'status_label', 'item_count', 'amount', 'created_at', 'created_at_local', 'address'],
      ...visibleOrders.map((order) => {
        const normalized = (order.status || '').trim().toLowerCase()
        const statusLabel = ORDER_STATUS_META[normalized]?.label || order.status || ''
        return [
          order.order_id,
          order.status,
          statusLabel,
          order.item_count ?? '',
          order.amount.toFixed(2),
          order.created_at,
          formatDateTime(order.created_at),
          order.address
        ]
      })
    ]
    const csv = rows.map((row) => row.map(escapeCSV).join(',')).join('\n')
    const blob = new Blob([`\ufeff${csv}`], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    const stamp = new Date().toISOString().replace(/[:.]/g, '-')
    link.href = url
    link.download = `orders-${stamp}.csv`
    link.click()
    URL.revokeObjectURL(url)
    setExportNotice(`已导出 ${visibleOrders.length} 条订单`)
  }

  async function handleCopyOrderId(orderId: number) {
    try {
      const ok = await copyTextToClipboard(String(orderId))
      if (ok) {
        setCopiedOrderId(orderId)
        setCopyNotice(`已复制订单号 ${orderId}`)
      } else {
        setCopiedOrderId(null)
        setCopyNotice('复制失败，请手动选择订单号')
      }
    } catch {
      setCopiedOrderId(null)
      setCopyNotice('复制失败，请手动选择订单号')
    }
  }

  if (!token) return <Navigate to="/login" replace />
  if (error) {
    return (
      <section className="panel error-panel">
        <h2>订单历史加载失败</h2>
        <p>{error}</p>
        <div className="error-actions">
        <button
          className="pill-btn"
          type="button"
          onClick={handleRefresh}
          disabled={loading || rangeInvalid || amountInputInvalid || amountRangeInvalid}
        >
          {loading ? '重试中...' : '重试加载'}
        </button>
          <button className="ghost-btn" type="button" onClick={clearAllFilters}>清空筛选</button>
        </div>
        <span className="meta-hint">若持续失败，可尝试缩小时间范围或稍后再试。</span>
      </section>
    )
  }

  return (
    <section className="panel">
      <h2>订单历史</h2>
      <div className="filters">
        <div className="status-filter">
          <span className="meta-label">订单状态</span>
          <div className="status-chips">
            {ORDER_STATUS_OPTIONS.map((opt) => {
              const isAll = !opt.value
              const isActive = isAll ? statusTokens.length === 0 : statusTokens.includes(opt.value)
              return (
                <button
                  key={opt.value || 'all'}
                  type="button"
                  className={`status-chip ${isActive ? 'active' : ''} ${isAll ? 'all' : ''}`}
                  aria-pressed={isActive}
                  onClick={() => toggleStatusFilter(opt.value)}
                >
                  {opt.label}
                </button>
              )
            })}
          </div>
          <span className="meta-hint">{statusHint}</span>
        </div>
        <input
          className={rangeInvalid ? 'invalid' : ''}
          type="datetime-local"
          value={from}
          onChange={(e) => { setFrom(e.target.value); resetPaging() }}
        />
        <input
          className={rangeInvalid ? 'invalid' : ''}
          type="datetime-local"
          value={to}
          onChange={(e) => { setTo(e.target.value); resetPaging() }}
        />
        <div className="filter-field">
          <input
            className={minAmountHasError ? 'invalid' : ''}
            type="number"
            min="0"
            step="0.01"
            inputMode="decimal"
            placeholder="最低金额"
            value={minAmount}
            aria-invalid={minAmountHasError}
            onChange={(e) => {
              setMinAmount(e.target.value)
              resetPaging()
            }}
          />
          {minAmountInvalid && <span className="field-hint">请输入非负数字</span>}
        </div>
        <div className="filter-field">
          <input
            className={maxAmountHasError ? 'invalid' : ''}
            type="number"
            min="0"
            step="0.01"
            inputMode="decimal"
            placeholder="最高金额"
            value={maxAmount}
            aria-invalid={maxAmountHasError}
            onChange={(e) => {
              setMaxAmount(e.target.value)
              resetPaging()
            }}
          />
          {maxAmountInvalid && <span className="field-hint">请输入非负数字</span>}
          {!maxAmountInvalid && amountRangeInvalid && (
            <span className="field-hint">最高金额需不低于最低金额</span>
          )}
        </div>
        <input
          type="search"
          placeholder="订单号搜索（支持逗号/空格）"
          value={orderQuery}
          onChange={(e) => {
            setOrderQuery(e.target.value)
            resetPaging()
          }}
        />
      </div>
      <div className="search-hint">
        <span className="meta-hint">
          订单号需为完整数字，支持多个，使用逗号或空格分隔（最多 {MAX_ORDER_ID_FILTERS} 个）。
        </span>
        <span className="meta-hint">
          金额范围支持输入 0 或小数，留空表示不限。
        </span>
      </div>
      {rangeInvalid && (
        <div className="filter-warning">
          <strong>时间范围不合法</strong>
          <span>开始时间不能晚于结束时间，请调整后再查询。</span>
        </div>
      )}
      {amountInputInvalid && (
        <div className="filter-warning">
          <strong>金额格式不正确</strong>
          <span>请输入非负数字金额，可包含小数。</span>
        </div>
      )}
      {amountRangeInvalid && (
        <div className="filter-warning">
          <strong>金额范围不合法</strong>
          <span>最低金额不能高于最高金额，请调整后再查询。</span>
        </div>
      )}
      {orderQueryHasInvalid && (
        <div className="filter-warning">
          <strong>订单号格式不正确</strong>
          <span>
            仅支持数字订单号，使用逗号或空格分隔。
            {hasSearch && ` 已忽略无效项：${invalidOrderTokens.join('、')}`}
          </span>
        </div>
      )}
      {orderQueryTooMany && (
        <div className="filter-warning">
          <strong>订单号数量过多</strong>
          <span>最多支持 {MAX_ORDER_ID_FILTERS} 个订单号，请精简后再查询。</span>
        </div>
      )}
      <div className="quick-range">
        <span className="meta-label">快捷时间</span>
        <button className="pill-btn" type="button" onClick={() => applyQuickRange(7)}>近 7 天</button>
        <button className="pill-btn" type="button" onClick={() => applyQuickRange(30)}>近 30 天</button>
        <button className="pill-btn" type="button" onClick={applyCurrentMonth}>本月</button>
      </div>
      <div className="page-size">
        <span className="meta-label">每页数量</span>
        <select
          value={pageSize}
          onChange={(e) => {
            const nextSize = Number(e.target.value)
            setPageSize(nextSize)
            localStorage.setItem(PAGE_SIZE_STORAGE_KEY, String(nextSize))
            resetPaging()
          }}
          aria-label="每页数量"
        >
          {PAGE_SIZE_OPTIONS.map((size) => (
            <option key={size} value={size}>每页 {size} 条</option>
          ))}
        </select>
      </div>
      <div className="total-toggle">
        <label>
          <input
            type="checkbox"
            checked={includeTotal}
            disabled={cursorMode}
            onChange={(e) => {
              const next = e.target.checked
              setIncludeTotal(next)
              localStorage.setItem(INCLUDE_TOTAL_STORAGE_KEY, String(next))
            }}
          />
          统计总订单数
        </label>
        <span className="meta-hint">{totalHint}</span>
      </div>
      <div className="filter-summary">
        <span>筛选：{filterPieces.join(' · ')}</span>
        {showClearSearch && (
          <button className="ghost-btn" type="button" onClick={clearSearch}>清空订单号</button>
        )}
        {showClearFilters && (
          <button className="ghost-btn" type="button" onClick={clearFilters}>清空筛选</button>
        )}
      </div>
      {copyNotice && <div className="copy-toast">{copyNotice}</div>}
      <div className="refresh-bar">
        <div>
          <span className="meta-label">最近刷新</span>
          <strong>{lastUpdatedLabel}</strong>
        </div>
        <button
          className="pill-btn"
          type="button"
          onClick={handleRefresh}
          disabled={loading || rangeInvalid || amountInputInvalid || amountRangeInvalid}
        >
          {loading ? '刷新中...' : '立即刷新'}
        </button>
      </div>
      <div className="export-bar">
        <div>
          <span className="meta-label">导出</span>
          <strong>{exportLabel}</strong>
          <span className="meta-hint">{exportHint}</span>
        </div>
        <button className="pill-btn" type="button" onClick={exportOrdersCSV} disabled={exportDisabled}>
          {exportDisabled ? '暂无可导出' : '导出 CSV'}
        </button>
      </div>
      <div className="order-insights">
        <div>
          <span className="meta-label">{pageCountLabel}</span>
          <strong>{visibleOrders.length}</strong>
        </div>
        <div>
          <span className="meta-label">{pageAmountLabel}</span>
          <strong>¥{pageAmount.toFixed(2)}</strong>
        </div>
        <div>
          <span className="meta-label">平均订单额</span>
          <strong>{averageAmountLabel}</strong>
        </div>
        <div>
          <span className="meta-label">总订单数</span>
          <strong>{totalLabel}</strong>
          {!includeTotal && !totalAvailable && <span className="meta-hint">未计算</span>}
          {!includeTotal && totalAvailable && <span className="meta-hint">缓存</span>}
        </div>
      </div>
      <p className="status-note">提示：状态筛选支持多选组合；订单号搜索会提交后端进行精确匹配；金额筛选支持区间输入；当前仅“已创建”状态由后端产生，其余状态为预留。</p>
      {loading && <p>订单加载中...</p>}
      {!loading && !rangeInvalid && !amountInputInvalid && !amountRangeInvalid && !orderQueryTooMany && visibleOrders.length === 0 && (
        <div className="empty-state">
          <h3>{hasActiveFilters ? '未找到匹配订单' : '暂无订单'}</h3>
          <p>
            {hasActiveFilters
              ? '没有匹配当前条件的订单，试试清空订单号搜索或调整筛选条件。'
              : '还没有订单记录，可以先去首页挑选心仪商品。'}
          </p>
          <div className="empty-actions">
            {showClearSearch && (
              <button className="ghost-btn" type="button" onClick={clearSearch}>清空订单号</button>
            )}
            {showClearFilters && (
              <button className="ghost-btn" type="button" onClick={clearFilters}>清空筛选</button>
            )}
            <Link className="pill-btn" to="/">去逛逛</Link>
          </div>
        </div>
      )}
      {!rangeInvalid && !amountInputInvalid && !amountRangeInvalid && visibleOrders.map((o) => (
        <div key={o.order_id} className="order-row">
          <div className="order-main">
            <span className="order-id">#{o.order_id}</span>
            <button
              className={`ghost-btn small-btn ${copiedOrderId === o.order_id ? 'copy-success' : ''}`}
              type="button"
              onClick={() => handleCopyOrderId(o.order_id)}
            >
              {copiedOrderId === o.order_id ? '已复制' : '复制订单号'}
            </button>
            <StatusTag status={o.status} />
            {typeof o.item_count === 'number' && (
              <span className="order-items-count">共{o.item_count}件</span>
            )}
          </div>
          <span className="order-amount">¥{o.amount.toFixed(2)}</span>
          <span className="order-date">{formatDateTime(o.created_at)}</span>
          <Link className="back-link" to={`/order/${o.order_id}`}>查看详情</Link>
        </div>
      ))}
      <div className="pagination">
        {canResetPaging && (
          <button className="ghost-btn small-btn" type="button" onClick={resetPaging} disabled={loading}>
            返回第一页
          </button>
        )}
        <button disabled={page <= 1 || loading} onClick={() => {
          const prevHistory = [...cursorHistory]
          const prevCursor = prevHistory.pop() || ''
          setCursorHistory(prevHistory)
          setCursor(prevCursor)
          setPage((p) => p - 1)
        }}>上一页</button>
        <span>
          {totalAvailable
            ? `第 ${page} 页 / 共 ${totalPages} 页 · 每页 ${pageSize} 条${includeTotal ? '' : ' · 缓存总数'}`
            : `第 ${page} 页 · 每页 ${pageSize} 条 · 总数未计算`}
        </span>
        <button disabled={loading || !nextCursor} onClick={() => {
          setCursorHistory((hist) => [...hist, cursor])
          setCursor(nextCursor)
          setPage((p) => p + 1)
        }}>下一页</button>
      </div>
    </section>
  )
}

export function App() {
  const navigate = useNavigate()
  const [token, setToken] = useState(localStorage.getItem('token') || '')
  const [products, setProducts] = useState<Product[]>([])
  const [cart, setCart] = useState<CartItem[]>([])
  const [cartTotal, setCartTotal] = useState(0)
  const [message, setMessage] = useState('')
  const [loading, setLoading] = useState(false)
  const [address, setAddress] = useState('Shanghai Pudong Road 1')
  const [retryIdemKey, setRetryIdemKey] = useState(localStorage.getItem(IDEM_STORAGE_KEY) || '')

  const cartCount = useMemo(() => cart.reduce((sum, item) => sum + item.quantity, 0), [cart])

  useEffect(() => {
    setUnauthorizedHandler(() => {
      localStorage.removeItem('token')
      setToken('')
      setCart([])
      setCartTotal(0)
      setMessage('会话已过期，请重新登录')
      navigate('/login')
    })
    return () => setUnauthorizedHandler(null)
  }, [navigate])

  useEffect(() => {
    setLoading(true)
    api.listProducts()
      .then(setProducts)
      .catch((e) => setMessage(e.message))
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => {
    if (token) {
      refreshCart().catch((e) => setMessage(e.message))
    }
  }, [token])

  async function refreshCart() {
    if (!token) return
    const data = await api.getCart(token)
    setCart(data.items)
    setCartTotal(data.total_amount)
  }

  async function addToCart(productId: number) {
    if (!token) {
      navigate('/login')
      return
    }
    setLoading(true)
    try {
      await api.addCartItem(token, productId, 1)
      await refreshCart()
      setMessage('已加入购物车')
    } catch (e) {
      setMessage((e as Error).message)
    } finally {
      setLoading(false)
    }
  }

  async function removeFromCart(productId: number) {
    if (!token) return
    setLoading(true)
    try {
      await api.removeCartItem(token, productId)
      await refreshCart()
    } catch (e) {
      setMessage((e as Error).message)
    } finally {
      setLoading(false)
    }
  }

  async function onLogin(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setLoading(true)
    setMessage('')
    const form = new FormData(e.currentTarget)
    const email = String(form.get('email') || '')
    const password = String(form.get('password') || '')
    try {
      const { token } = await api.login(email, password)
      localStorage.setItem('token', token)
      setToken(token)
      navigate('/')
    } catch (err) {
      setMessage((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  async function submitOrder(useRetryKey: boolean) {
    if (!token) return
    if (cart.length === 0) {
      setMessage('购物车为空，请先添加商品')
      return
    }
    const trimmedAddress = address.trim()
    if (!trimmedAddress) {
      setMessage('收货地址不能为空')
      return
    }
    if (trimmedAddress.length > 200) {
      setMessage('收货地址过长（最多 200 字）')
      return
    }

    const idemKey = useRetryKey && retryIdemKey ? retryIdemKey : `web-${Date.now()}`
    localStorage.setItem(IDEM_STORAGE_KEY, idemKey)
    setRetryIdemKey(idemKey)

    setMessage('')
    setLoading(true)
    try {
      const order = await api.placeOrder(token, trimmedAddress, idemKey)
      await refreshCart()
      localStorage.removeItem(IDEM_STORAGE_KEY)
      setRetryIdemKey('')
      setMessage(order.idempotent_replay ? '重复请求已命中幂等结果' : '下单成功，订单已创建')
      const replayKey = `${REPLAY_STORAGE_PREFIX}${order.order_id}`
      if (order.idempotent_replay) {
        sessionStorage.setItem(replayKey, 'true')
      } else {
        sessionStorage.removeItem(replayKey)
      }
      navigate(`/order/${order.order_id}`, { state: { idempotentReplay: order.idempotent_replay } })
    } catch (e) {
      setMessage(`${(e as Error).message}（可使用固定幂等键重试）`)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="page">
      <TopNav cartCount={cartCount} token={token} />
      <Loading show={loading} />
      {message && <div className="error-banner">{message}</div>}

      <Routes>
        <Route path="/" element={
          <section>
            <h2>推荐商品 ({products.length})</h2>
            <div className="grid">
              {products.map((p) => (
                <article key={p.id} className="card">
                  <h3>{p.title}</h3>
                  <p className="price">¥{p.price}</p>
                  <p className="stock">库存 {p.stock}</p>
                  <button onClick={() => addToCart(p.id)}>加入购物车</button>
                </article>
              ))}
            </div>
          </section>
        } />

        <Route path="/login" element={
          <section className="panel">
            <h2>登录</h2>
            <form onSubmit={onLogin} className="form">
              <input required name="email" type="email" placeholder="邮箱" defaultValue="itest@example.com" />
              <input required name="password" type="password" placeholder="密码" defaultValue="123456" />
              <button disabled={loading} type="submit">{loading ? '登录中...' : '登录'}</button>
            </form>
          </section>
        } />

        <Route path="/cart" element={
          <section className="panel">
            <h2>购物车</h2>
            {cart.length === 0 && <p>购物车为空</p>}
            {cart.map((item) => (
              <div key={item.product_id} className="cart-row">
                <span>{item.title}</span>
                <span>x{item.quantity}</span>
                <span>¥{item.price}</span>
                <button onClick={() => removeFromCart(item.product_id)}>删除</button>
              </div>
            ))}
            <div className="checkout">
              <strong>合计：¥{cartTotal.toFixed(2)}</strong>
              <input value={address} onChange={(e) => setAddress(e.target.value)} placeholder="收货地址" />
              <small className="hint">地址必填，最长 200 字</small>
              <button disabled={cart.length === 0 || loading} onClick={() => submitOrder(false)}>立即下单</button>
              {retryIdemKey && (
                <button className="retry-btn" disabled={cart.length === 0 || loading} onClick={() => submitOrder(true)}>
                  重试下单（复用幂等键）
                </button>
              )}
              {retryIdemKey && <small className="hint">当前重试幂等键：{retryIdemKey}</small>}
            </div>
          </section>
        } />

        <Route path="/orders" element={<OrdersPage token={token} />} />
        <Route path="/order/:orderId" element={<OrderDetailPage token={token} />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  )
}
