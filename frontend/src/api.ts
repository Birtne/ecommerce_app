const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8080/api/v1'

export type Product = { id: number; title: string; price: number; stock: number }
export type CartItem = { product_id: number; title: string; quantity: number; price: number }
export type OrderResponse = { order_id: number; amount: number; status: string; idempotent_replay: boolean }
export type OrderSummary = { order_id: number; user_id: number; address: string; amount: number; status: string; created_at: string; item_count?: number }
export type PagedOrders = { items: OrderSummary[]; page: number; page_size: number; total: number; next_cursor?: string }
export type OrderFilter = {
  status?: string
  from?: string
  to?: string
  minAmount?: number
  maxAmount?: number
  page?: number
  pageSize?: number
  cursor?: string
  includeTotal?: boolean
  orderIds?: number[]
}
export type OrderDetail = {
  order_id: number
  user_id: number
  address: string
  amount: number
  status: string
  created_at?: string
  item_count?: number
  idempotency_key?: string
  idempotency_created_at?: string
  idempotency_last_replay_at?: string
  items: Array<{ product_id: number; title: string; price: number; quantity: number; subtotal?: number }>
}

let unauthorizedHandler: (() => void) | null = null

const ERROR_MAP: Record<string, string> = {
  'empty cart': '购物车为空，请先添加商品',
  'idempotency key is being processed': '订单正在处理中，请勿重复提交',
  'address is required': '收货地址不能为空',
  'address too long': '收货地址过长（最多 200 字）',
  'idempotency key too long': '幂等键过长，请稍后重试',
  'idempotency key must not contain spaces': '幂等键不能包含空格',
  'invalid payload': '请求格式错误',
  'invalid product_id': '商品信息无效，请刷新后重试',
  'invalid order_id': '订单号无效',
  'order not found': '未找到订单',
  'invalid min_amount, expected non-negative number': '最低金额格式不正确，请输入非负数字',
  'invalid max_amount, expected non-negative number': '最高金额格式不正确，请输入非负数字',
  'invalid amount range: min_amount greater than max_amount': '金额范围不合法，最低金额不能高于最高金额',
  'unauthorized': '请先登录'
}

function resolveErrorMessage(raw: string): string {
  const key = raw.trim().toLowerCase()
  if (key.startsWith('invalid order_ids')) return '订单号格式不正确（仅支持数字，逗号/空格分隔）'
  if (key.startsWith('too many order_ids')) return '订单号数量过多（最多 50 个）'
  return ERROR_MAP[key] || raw
}

export function setUnauthorizedHandler(handler: (() => void) | null) {
  unauthorizedHandler = handler
}

async function request<T>(path: string, init: RequestInit = {}, token?: string): Promise<T> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (token) headers.Authorization = `Bearer ${token}`
  const res = await fetch(`${API_BASE}${path}`, { ...init, headers: { ...headers, ...(init.headers || {}) } })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    if (res.status === 401 && unauthorizedHandler) {
      unauthorizedHandler()
      throw new Error('会话已过期，请重新登录')
    }
    const raw = body.error || `Request failed: ${res.status}`
    throw new Error(resolveErrorMessage(String(raw)))
  }
  return res.json() as Promise<T>
}

export const api = {
  register: (email: string, password: string, name: string) => request<{ token: string }>('/auth/register', { method: 'POST', body: JSON.stringify({ email, password, name }) }),
  login: (email: string, password: string) => request<{ token: string }>('/auth/login', { method: 'POST', body: JSON.stringify({ email, password }) }),
  listProducts: () => request<Product[]>('/products'),
  addCartItem: (token: string, productId: number, quantity: number) => request<{ ok: boolean }>('/cart/items', { method: 'POST', body: JSON.stringify({ product_id: productId, quantity }) }, token),
  getCart: (token: string) => request<{ items: CartItem[]; total_amount: number }>('/cart', { method: 'GET' }, token),
  removeCartItem: (token: string, productId: number) => request<{ ok: boolean }>(`/cart/items/${productId}`, { method: 'DELETE' }, token),
  placeOrder: (token: string, address: string, idempotencyKey: string) =>
    request<OrderResponse>('/orders', {
      method: 'POST',
      headers: { 'Idempotency-Key': idempotencyKey },
      body: JSON.stringify({ address })
    }, token),
  listOrders: (token: string, filter: OrderFilter = {}) => {
    const q = new URLSearchParams()
    if (filter.status) q.set('status', filter.status)
    if (filter.orderIds && filter.orderIds.length > 0) q.set('order_ids', filter.orderIds.join(','))
    if (filter.from) q.set('from', filter.from)
    if (filter.to) q.set('to', filter.to)
    if (filter.minAmount !== undefined) q.set('min_amount', String(filter.minAmount))
    if (filter.maxAmount !== undefined) q.set('max_amount', String(filter.maxAmount))
    if (filter.page) q.set('page', String(filter.page))
    if (filter.pageSize) q.set('page_size', String(filter.pageSize))
    if (filter.cursor) q.set('cursor', filter.cursor)
    if (filter.includeTotal !== undefined) q.set('include_total', String(filter.includeTotal))
    const qs = q.toString()
    return request<PagedOrders>(`/orders${qs ? `?${qs}` : ''}`, { method: 'GET' }, token)
  },
  getOrder: (token: string, orderId: number) => request<OrderDetail>(`/orders/${orderId}`, { method: 'GET' }, token)
}
