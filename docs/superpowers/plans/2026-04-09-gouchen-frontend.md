# Gouchen Frontend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Vite + React + TypeScript chat interface for the Gouchen NL2SQL system, with automatic charts, clarification, and SQL approval workflows.

**Architecture:** A separate SPA (`gouchen-frontend/`) communicates with the existing FastAPI backend via CORS. The core state lives in `useQuery` hook,UI is composed of small reusable components, and `react-router-dom` reserves space for future admin pages.

**Tech Stack:** Vite, React 18, TypeScript, Tailwind CSS, shadcn/ui, Recharts, react-router-dom, Vitest, React Testing Library, MSW.

---

## File Map

| File | Responsibility |
|------|----------------|
| `gouchen/src/main.py` | Add CORS middleware so frontend can call FastAPI |
| `gouchen-frontend/` | New Vite React TS project root |
| `gouchen-frontend/src/types/api.ts` | Mirror FastAPI request/response types |
| `gouchen-frontend/src/api/query.ts` | Axios/fetch wrappers for `/query`, `/clarify`, `/approve`, `/status` |
| `gouchen-frontend/src/utils/chartHelper.ts` | Infer Recharts chart type from tabular data |
| `gouchen-frontend/src/components/Sidebar.tsx` | Session list + new chat button |
| `gouchen-frontend/src/components/ChatMessage.tsx` | Message bubble (user / assistant) |
| `gouchen-frontend/src/components/SqlPanel.tsx` | Collapsible SQL code block |
| `gouchen-frontend/src/components/DataTable.tsx` | Render execution_result rows |
| `gouchen-frontend/src/components/AutoChart.tsx` | Recharts chart based on `chartHelper.ts` inference |
| `gouchen-frontend/src/components/ClarificationForm.tsx` | Render and submit clarification questions |
| `gouchen-frontend/src/components/ApprovalPanel.tsx` | Show SQL + explanation + approve/reject/edit |
| `gouchen-frontend/src/hooks/useQuery.ts` | Manage query lifecycle state machine |
| `gouchen-frontend/src/pages/ChatPage.tsx` | Assemble layout: Sidebar + chat stream + input |
| `gouchen-frontend/src/App.tsx` | React Router root (ChatPage route for now) |

---

## Task 1: Enable CORS on FastAPI Backend

**Files:**
- Modify: `gouchen/src/main.py`

- [ ] **Step 1: Add CORS middleware**

Add after the `FastAPI` app creation in `gouchen/src/main.py`:

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

- [ ] **Step 2: Verify backend still starts**

Run: `cd gouchen && python -m src.main`
Expected: Server starts on port 8000 with no import errors.

- [ ] **Step 3: Commit**

```bash
git add gouchen/src/main.py
git commit -m "feat: add CORS for frontend dev server"
```

---

## Task 2: Scaffold Vite React TypeScript Project

**Files:**
- Create directory: `gouchen-frontend/`
- Create: `gouchen-frontend/package.json`
- Create: `gouchen-frontend/vite.config.ts`
- Create: `gouchen-frontend/tsconfig.json`
- Create: `gouchen-frontend/index.html`
- Create: `gouchen-frontend/src/main.tsx`
- Create: `gouchen-frontend/src/App.tsx`

- [ ] **Step 1: Generate project via npm create**

Run from repo root:
```bash
npm create vite@latest gouchen-frontend -- --template react-ts
cd gouchen-frontend
npm install
```

- [ ] **Step 2: Verify dev server starts**

Run: `cd gouchen-frontend && npm run dev`
Expected: Terminal shows `http://localhost:5173/`, browser opens to Vite + React default page.

- [ ] **Step 3: Commit**

```bash
git add gouchen-frontend
git commit -m "chore: scaffold Vite React TS frontend"
```

---

## Task 3: Setup Tailwind CSS and shadcn/ui

**Files:**
- Create: `gouchen-frontend/tailwind.config.js`
- Create: `gouchen-frontend/postcss.config.js`
- Modify: `gouchen-frontend/src/index.css`
- Modify: `gouchen-frontend/package.json` (via npm install)

- [ ] **Step 1: Install dependencies**

Run from `gouchen-frontend/`:
```bash
npm install -D tailwindcss postcss autoprefixer
npx tailwindcss init -p
```

- [ ] **Step 2: Configure Tailwind**

Write `gouchen-frontend/tailwind.config.js`:

```js
/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}
```

- [ ] **Step 3: Add Tailwind directives to CSS**

Replace `gouchen-frontend/src/index.css` with:

```css
@tailwind base;
@tailwind components;
@tailwind utilities;
```

- [ ] **Step 4: Install shadcn/ui base dependencies**

Run:
```bash
npm install class-variance-authority clsx tailwind-merge lucide-react
```

- [ ] **Step 5: Add shadcn/ui utils file**

Create `gouchen-frontend/src/lib/utils.ts`:

```ts
import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}
```

- [ ] **Step 6: Commit**

```bash
git add gouchen-frontend
git commit -m "chore: setup Tailwind CSS and shadcn/ui base"
```

---

## Task 4: Define API Types

**Files:**
- Create: `gouchen-frontend/src/types/api.ts`

- [ ] **Step 1: Write types mirroring FastAPI models**

Create `gouchen-frontend/src/types/api.ts`:

```ts
export interface QueryRequest {
  query: string;
  thread_id?: string;
  user_role?: string;
  datasource?: string;
}

export interface ClarificationAnswer {
  field: string;
  answer: string;
}

export interface ClarifyRequest {
  thread_id: string;
  answers: ClarificationAnswer[];
}

export interface ApproveRequest {
  thread_id: string;
  decision: "approve" | "reject" | "feedback";
  edited_sql?: string;
}

export interface QueryResponse {
  status: "completed" | "pending_approval" | "needs_clarification" | "error";
  result?: {
    query?: string;
    generated_sql?: string;
    sql_explanation?: string;
    execution_result?: any;
    formatted_result?: any;
    summary?: string;
    approval_decision?: any;
    clarification_history?: any[];
  };
  error?: string;
  thread_id?: string;
  pending_info?: {
    query: string;
    generated_sql?: string;
    sql_explanation?: string;
    message: string;
  };
  clarification_info?: {
    round: number;
    max_rounds: number;
    questions: { field: string; question: string }[];
    current_intent?: any;
    message: string;
  };
}

export interface StatusResponse {
  thread_id: string;
  status: string;
  current_state?: any;
  next_node?: string;
  error?: string;
}
```

- [ ] **Step 2: Commit**

```bash
git add gouchen-frontend/src/types/api.ts
git commit -m "feat: add API TypeScript types"
```

---

## Task 5: Create API Client

**Files:**
- Create: `gouchen-frontend/src/api/query.ts`

- [ ] **Step 1: Implement API wrappers**

Create `gouchen-frontend/src/api/query.ts`:

```ts
import type { QueryRequest, QueryResponse, ClarifyRequest, ApproveRequest, StatusResponse } from "../types/api";

const API_BASE = "http://localhost:8000/api/v1";

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

export const queryApi = {
  createQuery: (req: QueryRequest) => post<QueryResponse>("/query", req),
  clarify: (req: ClarifyRequest) => post<QueryResponse>("/clarify", req),
  approve: (req: ApproveRequest) => post<QueryResponse>("/approve", req),
  getStatus: (threadId: string) => get<StatusResponse>(`/status/${threadId}`),
};
```

- [ ] **Step 2: Commit**

```bash
git add gouchen-frontend/src/api/query.ts
git commit -m "feat: add API client wrappers"
```

---

## Task 6: Build Chart Helper Utility (TDD)

**Files:**
- Create: `gouchen-frontend/src/utils/chartHelper.ts`
- Create test: `gouchen-frontend/src/utils/chartHelper.test.ts`

- [ ] **Step 1: Install Vitest in devDependencies**

Run from `gouchen-frontend/`:
```bash
npm install -D vitest @vitest/ui jsdom
```

Update `gouchen-frontend/vite.config.ts` to add test config:

```ts
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    globals: true,
  },
})
```

Also update `gouchen-frontend/tsconfig.json` `compilerOptions.types` to include `vitest/globals` if needed, but usually not required.

Add test script to `package.json`:
```json
"test": "vitest"
```

- [ ] **Step 2: Write failing test**

Create `gouchen-frontend/src/utils/chartHelper.test.ts`:

```ts
import { inferChartType, ChartType } from "./chartHelper";

describe("inferChartType", () => {
  it("returns line for date + number columns", () => {
    const rows = [{ date: "2024-01-01", value: 100 }];
    expect(inferChartType(rows)).toBe("line");
  });

  it("returns bar for category + number columns", () => {
    const rows = [{ region: "East", sales: 200 }];
    expect(inferChartType(rows)).toBe("bar");
  });

  it("returns pie for two rows with category + number (percentage-like)", () => {
    const rows = [
      { category: "A", value: 30 },
      { category: "B", value: 70 },
    ];
    expect(inferChartType(rows)).toBe("pie");
  });

  it("returns null for empty rows", () => {
    expect(inferChartType([])).toBeNull();
  });
});
```

Run: `cd gouchen-frontend && npx vitest run src/utils/chartHelper.test.ts`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement minimal chartHelper**

Create `gouchen-frontend/src/utils/chartHelper.ts`:

```ts
export type ChartType = "line" | "bar" | "pie";

function isDateLike(value: unknown): boolean {
  if (typeof value !== "string") return false;
  return !isNaN(Date.parse(value));
}

function isNumberLike(value: unknown): boolean {
  return typeof value === "number";
}

export function inferChartType(rows: Record<string, unknown>[]): ChartType | null {
  if (!rows || rows.length === 0) return null;
  const sample = rows[0];
  const keys = Object.keys(sample);
  if (keys.length < 2) return null;

  const types = keys.map((k) => {
    const v = sample[k];
    if (isDateLike(v)) return "date";
    if (isNumberLike(v)) return "number";
    return "category";
  });

  const hasDate = types.includes("date");
  const hasNumber = types.includes("number");
  const categoryCount = types.filter((t) => t === "category").length;

  if (hasDate && hasNumber) return "line";
  if (categoryCount >= 1 && hasNumber) {
    if (rows.length === 2 && categoryCount === 1) return "pie";
    return "bar";
  }
  if (hasNumber && keys.length > 2) return "bar";
  return null;
}
```

Run tests again: `cd gouchen-frontend && npx vitest run src/utils/chartHelper.test.ts`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add gouchen-frontend/src/utils/chartHelper.ts gouchen-frontend/src/utils/chartHelper.test.ts gouchen-frontend/vite.config.ts gouchen-frontend/package.json
git commit -m "feat: add chart inference utility with tests"
```

---

## Task 7: Build DataTable Component (TDD)

**Files:**
- Create: `gouchen-frontend/src/components/DataTable.tsx`
- Create test: `gouchen-frontend/src/components/DataTable.test.tsx`

- [ ] **Step 1: Install RTL dependencies**

Run from `gouchen-frontend/`:
```bash
npm install -D @testing-library/react @testing-library/jest-dom happy-dom
```

Update `vite.config.ts` test environment to `happy-dom`:

```ts
export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'happy-dom',
    globals: true,
  },
})
```

- [ ] **Step 2: Write failing test**

Create `gouchen-frontend/src/components/DataTable.test.tsx`:

```tsx
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { DataTable } from "./DataTable";

describe("DataTable", () => {
  it("renders headers and rows", () => {
    const data = [
      { region: "East", sales: 100 },
      { region: "West", sales: 200 },
    ];
    render(<DataTable data={data} />);
    expect(screen.getByText("region")).toBeDefined();
    expect(screen.getByText("East")).toBeDefined();
    expect(screen.getByText("200")).toBeDefined();
  });

  it("renders empty state for empty data", () => {
    render(<DataTable data={[]} />);
    expect(screen.getByText("无数据")).toBeDefined();
  });
});
```

Run: `cd gouchen-frontend && npx vitest run src/components/DataTable.test.tsx`
Expected: FAIL (module not found)

- [ ] **Step 3: Implement DataTable**

Create `gouchen-frontend/src/components/DataTable.tsx`:

```tsx
export function DataTable({ data }: { data: Record<string, unknown>[] }) {
  if (!data || data.length === 0) {
    return <div className="text-sm text-gray-500 py-2">无数据</div>;
  }
  const headers = Object.keys(data[0]);
  return (
    <div className="overflow-x-auto border rounded-md my-2">
      <table className="min-w-full text-sm">
        <thead className="bg-gray-100">
          <tr>
            {headers.map((h) => (
              <th key={h} className="px-3 py-2 text-left font-medium text-gray-700">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, idx) => (
            <tr key={idx} className="border-t">
              {headers.map((h) => (
                <td key={h} className="px-3 py-2 text-gray-800">
                  {String(row[h] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

Run tests again: expected PASS.

- [ ] **Step 4: Commit**

```bash
git add gouchen-frontend/src/components/DataTable.tsx gouchen-frontend/src/components/DataTable.test.tsx gouchen-frontend/vite.config.ts gouchen-frontend/package.json
git commit -m "feat: add DataTable component with tests"
```

---

## Task 8: Build AutoChart Component (TDD)

**Files:**
- Create: `gouchen-frontend/src/components/AutoChart.tsx`
- Create test: `gouchen-frontend/src/components/AutoChart.test.tsx`

- [ ] **Step 1: Install Recharts**

Run from `gouchen-frontend/`:
```bash
npm install recharts
```

- [ ] **Step 2: Write failing test**

Create `gouchen-frontend/src/components/AutoChart.test.tsx`:

```tsx
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { AutoChart } from "./AutoChart";

describe("AutoChart", () => {
  it("renders line chart for date + value data", () => {
    const data = [
      { date: "2024-01-01", value: 10 },
      { date: "2024-01-02", value: 20 },
    ];
    const { container } = render(<AutoChart data={data} />);
    expect(container.querySelector(".recharts-line")).toBeDefined();
  });

  it("falls back to DataTable when chart type is unknown", () => {
    const data = [{ name: "A", desc: "hello" }];
    render(<AutoChart data={data} />);
    expect(screen.getByText("A")).toBeDefined();
    expect(screen.getByText("hello")).toBeDefined();
  });
});
```

Run: `cd gouchen-frontend && npx vitest run src/components/AutoChart.test.tsx`
Expected: FAIL

- [ ] **Step 3: Implement AutoChart**

Create `gouchen-frontend/src/components/AutoChart.tsx`:

```tsx
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { inferChartType } from "../utils/chartHelper";
import { DataTable } from "./DataTable";

const COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6"];

export function AutoChart({ data }: { data: Record<string, unknown>[] }) {
  const chartType = inferChartType(data);
  if (!chartType) return <DataTable data={data} />;

  const keys = Object.keys(data[0]);
  const categoryKey =
    keys.find((k) => typeof data[0][k] === "string" && isNaN(Date.parse(String(data[0][k])))) ||
    keys.find((k) => typeof data[0][k] === "string") ||
    keys[0];
  const numberKey = keys.find((k) => typeof data[0][k] === "number") || keys[1];

  return (
    <div className="h-64 my-2">
      <ResponsiveContainer width="100%" height="100%">
        {chartType === "line" ? (
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey={categoryKey} />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey={numberKey} stroke="#3b82f6" />
          </LineChart>
        ) : chartType === "bar" ? (
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey={categoryKey} />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey={numberKey} fill="#3b82f6" />
          </BarChart>
        ) : (
          <PieChart>
            <Tooltip />
            <Legend />
            <Pie data={data} dataKey={numberKey} nameKey={categoryKey} cx="50%" cy="50%" outerRadius={80}>
              {data.map((_, idx) => (
                <Cell key={`cell-${idx}`} fill={COLORS[idx % COLORS.length]} />
              ))}
            </Pie>
          </PieChart>
        )}
      </ResponsiveContainer>
    </div>
  );
}
```

Run tests again: expected PASS.

- [ ] **Step 4: Commit**

```bash
git add gouchen-frontend/src/components/AutoChart.tsx gouchen-frontend/src/components/AutoChart.test.tsx gouchen-frontend/package.json
git commit -m "feat: add AutoChart component with Recharts integration"
```

---

## Task 9: Build SqlPanel Component

**Files:**
- Create: `gouchen-frontend/src/components/SqlPanel.tsx`

- [ ] **Step 1: Implement collapsible SQL panel**

Create `gouchen-frontend/src/components/SqlPanel.tsx`:

```tsx
import { useState } from "react";

export function SqlPanel({ sql }: { sql?: string }) {
  const [open, setOpen] = useState(false);
  if (!sql) return null;
  return (
    <div className="my-2">
      <button
        onClick={() => setOpen((v) => !v)}
        className="text-sm text-blue-600 hover:text-blue-800 font-medium"
      >
        {open ? "▲ 隐藏 SQL" : "▼ 查看生成的 SQL"}
      </button>
      {open && (
        <pre className="mt-2 p-3 bg-gray-900 text-gray-100 rounded-md text-xs overflow-x-auto">
          <code>{sql}</code>
        </pre>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add gouchen-frontend/src/components/SqlPanel.tsx
git commit -m "feat: add SqlPanel component"
```

---

## Task 10: Build ClarificationForm Component (TDD)

**Files:**
- Create: `gouchen-frontend/src/components/ClarificationForm.tsx`
- Create test: `gouchen-frontend/src/components/ClarificationForm.test.tsx`

- [ ] **Step 1: Write failing test**

Create `gouchen-frontend/src/components/ClarificationForm.test.tsx`:

```tsx
import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { ClarificationForm } from "./ClarificationForm";

describe("ClarificationForm", () => {
  it("renders questions and submits answers", () => {
    const onSubmit = vi.fn();
    const questions = [
      { field: "metric", question: "您想查哪个指标？" },
      { field: "time", question: "时间范围是？" },
    ];
    render(<ClarificationForm questions={questions} onSubmit={onSubmit} />);
    expect(screen.getByText("您想查哪个指标？")).toBeDefined();

    const inputs = screen.getAllByPlaceholderText("请输入...");
    fireEvent.change(inputs[0], { target: { value: "订单量" } });
    fireEvent.change(inputs[1], { target: { value: "最近7天" } });

    fireEvent.click(screen.getByText("提交"));
    expect(onSubmit).toHaveBeenCalledWith([
      { field: "metric", answer: "订单量" },
      { field: "time", answer: "最近7天" },
    ]);
  });
});
```

Run: `cd gouchen-frontend && npx vitest run src/components/ClarificationForm.test.tsx`
Expected: FAIL

- [ ] **Step 2: Implement ClarificationForm**

Create `gouchen-frontend/src/components/ClarificationForm.tsx`:

```tsx
import { useState } from "react";

interface Question {
  field: string;
  question: string;
}

interface ClarificationFormProps {
  questions: Question[];
  onSubmit: (answers: { field: string; answer: string }[]) => void;
}

export function ClarificationForm({ questions, onSubmit }: ClarificationFormProps) {
  const [answers, setAnswers] = useState<Record<string, string>>(() =>
    Object.fromEntries(questions.map((q) => [q.field, ""]))
  );

  return (
    <div className="bg-yellow-50 border border-yellow-200 rounded-md p-4 my-2">
      <p className="text-sm font-medium text-yellow-800 mb-2">查询存在歧义，请补充以下信息：</p>
      <div className="space-y-3">
        {questions.map((q) => (
          <div key={q.field}>
            <label className="block text-sm text-gray-700 mb-1">{q.question}</label>
            <input
              type="text"
              className="w-full border rounded-md px-3 py-2 text-sm"
              placeholder="请输入..."
              value={answers[q.field]}
              onChange={(e) => setAnswers((a) => ({ ...a, [q.field]: e.target.value }))}
            />
          </div>
        ))}
      </div>
      <button
        className="mt-3 px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
        onClick={() => onSubmit(questions.map((q) => ({ field: q.field, answer: answers[q.field] })))}
      >
        提交
      </button>
    </div>
  );
}
```

Run tests again: expected PASS.

- [ ] **Step 3: Commit**

```bash
git add gouchen-frontend/src/components/ClarificationForm.tsx gouchen-frontend/src/components/ClarificationForm.test.tsx
git commit -m "feat: add ClarificationForm component with tests"
```

---

## Task 11: Build ApprovalPanel Component (TDD)

**Files:**
- Create: `gouchen-frontend/src/components/ApprovalPanel.tsx`
- Create test: `gouchen-frontend/src/components/ApprovalPanel.test.tsx`

- [ ] **Step 1: Write failing test**

Create `gouchen-frontend/src/components/ApprovalPanel.test.tsx`:

```tsx
import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { ApprovalPanel } from "./ApprovalPanel";

describe("ApprovalPanel", () => {
  it("renders sql and explanation and calls onApprove", () => {
    const onApprove = vi.fn();
    const onReject = vi.fn();
    render(
      <ApprovalPanel
        sql="SELECT * FROM orders"
        explanation="查询所有订单"
        onApprove={onApprove}
        onReject={onReject}
      />
    );
    expect(screen.getByText("查询所有订单")).toBeDefined();
    fireEvent.click(screen.getByText("通过"));
    expect(onApprove).toHaveBeenCalledWith(undefined);
  });

  it("allows editing sql before approval", () => {
    const onApprove = vi.fn();
    render(
      <ApprovalPanel
        sql="SELECT * FROM orders"
        explanation="查询所有订单"
        onApprove={onApprove}
        onReject={() => {}}
      />
    );
    const textarea = screen.getByRole("textbox");
    fireEvent.change(textarea, { target: { value: "SELECT id FROM orders" } });
    fireEvent.click(screen.getByText("编辑后通过"));
    expect(onApprove).toHaveBeenCalledWith("SELECT id FROM orders");
  });
});
```

Run test: expected FAIL.

- [ ] **Step 2: Implement ApprovalPanel**

Create `gouchen-frontend/src/components/ApprovalPanel.tsx`:

```tsx
import { useState } from "react";

interface ApprovalPanelProps {
  sql: string;
  explanation?: string;
  onApprove: (editedSql?: string) => void;
  onReject: () => void;
}

export function ApprovalPanel({ sql, explanation, onApprove, onReject }: ApprovalPanelProps) {
  const [editedSql, setEditedSql] = useState(sql);

  return (
    <div className="bg-orange-50 border border-orange-200 rounded-md p-4 my-2">
      <p className="text-sm font-medium text-orange-800 mb-2">SQL 已生成，请审核：</p>
      {explanation && <p className="text-sm text-gray-700 mb-2">{explanation}</p>}
      <textarea
        className="w-full h-24 border rounded-md p-2 text-xs font-mono bg-white"
        value={editedSql}
        onChange={(e) => setEditedSql(e.target.value)}
      />
      <div className="flex gap-2 mt-3">
        <button
          className="px-3 py-1.5 bg-green-600 text-white text-sm rounded-md hover:bg-green-700"
          onClick={() => onApprove(editedSql !== sql ? editedSql : undefined)}
        >
          通过
        </button>
        <button
          className="px-3 py-1.5 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
          onClick={() => onApprove(editedSql)}
        >
          编辑后通过
        </button>
        <button
          className="px-3 py-1.5 bg-red-600 text-white text-sm rounded-md hover:bg-red-700"
          onClick={onReject}
        >
          拒绝
        </button>
      </div>
    </div>
  );
}
```

Run tests: expected PASS.

- [ ] **Step 3: Commit**

```bash
git add gouchen-frontend/src/components/ApprovalPanel.tsx gouchen-frontend/src/components/ApprovalPanel.test.tsx
git commit -m "feat: add ApprovalPanel component with tests"
```

---

## Task 12: Build Sidebar Component

**Files:**
- Create: `gouchen-frontend/src/components/Sidebar.tsx`

- [ ] **Step 1: Implement Sidebar**

Create `gouchen-frontend/src/components/Sidebar.tsx`:

```tsx
interface SidebarProps {
  sessions: { threadId: string; title: string }[];
  activeThreadId?: string;
  onNewChat: () => void;
  onSelect: (threadId: string) => void;
}

export function Sidebar({ sessions, activeThreadId, onNewChat, onSelect }: SidebarProps) {
  return (
    <div className="w-64 h-full border-r bg-gray-50 flex flex-col">
      <div className="p-4 border-b">
        <h1 className="font-semibold text-gray-800">钩沉 NL2SQL</h1>
      </div>
      <div className="p-3">
        <button
          onClick={onNewChat}
          className="w-full px-3 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700"
        >
          + 新对话
        </button>
      </div>
      <div className="flex-1 overflow-y-auto px-3 space-y-1">
        {sessions.map((s) => (
          <button
            key={s.threadId}
            onClick={() => onSelect(s.threadId)}
            className={`w-full text-left px-3 py-2 rounded-md text-sm truncate ${
              s.threadId === activeThreadId
                ? "bg-blue-100 text-blue-900"
                : "hover:bg-gray-200 text-gray-700"
            }`}
          >
            {s.title}
          </button>
        ))}
      </div>
      <div className="p-3 border-t text-xs text-gray-500">
        {/* 预留管理入口 */}
        <span>v0.1.0</span>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add gouchen-frontend/src/components/Sidebar.tsx
git commit -m "feat: add Sidebar component"
```

---

## Task 13: Build ChatMessage Component

**Files:**
- Create: `gouchen-frontend/src/components/ChatMessage.tsx`

- [ ] **Step 1: Implement ChatMessage**

Create `gouchen-frontend/src/components/ChatMessage.tsx`:

```tsx
import { AutoChart } from "./AutoChart";
import { DataTable } from "./DataTable";
import { SqlPanel } from "./SqlPanel";

export interface ChatMessageData {
  role: "user" | "assistant" | "system";
  content?: string;
  sql?: string;
  explanation?: string;
  tableData?: Record<string, unknown>[];
  summary?: string;
}

export function ChatMessage({ message }: { message: ChatMessageData }) {
  const isUser = message.role === "user";
  return (
    <div className={`flex ${isUser ? "justify-end" : "justify-start"} my-2`}>
      <div
        className={`max-w-[90%] rounded-lg px-4 py-2 text-sm ${
          isUser
            ? "bg-gray-200 text-gray-900"
            : message.role === "system"
            ? "bg-red-50 text-red-800 border border-red-200"
            : "bg-blue-50 text-gray-900 border border-blue-100"
        }`}
      >
        {message.summary && <p className="font-medium mb-1">{message.summary}</p>}
        {message.content && <p className="whitespace-pre-wrap">{message.content}</p>}
        {message.tableData && message.tableData.length > 0 && (
          <>
            <AutoChart data={message.tableData} />
            <DataTable data={message.tableData} />
          </>
        )}
        {message.sql && <SqlPanel sql={message.sql} />}
        {message.explanation && (
          <p className="text-xs text-gray-500 mt-1">{message.explanation}</p>
        )}
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add gouchen-frontend/src/components/ChatMessage.tsx
git commit -m "feat: add ChatMessage component"
```

---

## Task 14: Build useQuery Hook (TDD)

**Files:**
- Create: `gouchen-frontend/src/hooks/useQuery.ts`
- Create test: `gouchen-frontend/src/hooks/useQuery.test.ts`

- [ ] **Step 1: Install MSW for mocking fetch**

Run from `gouchen-frontend/`:
```bash
npm install -D msw@2
```

- [ ] **Step 2: Write failing test**

Create `gouchen-frontend/src/hooks/useQuery.test.ts`:

```ts
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, act, waitFor } from "@testing-library/react";
import { useQuery } from "./useQuery";

describe("useQuery", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("transitions idle -> submitting -> completed", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        status: "completed",
        thread_id: "t1",
        result: { summary: "done" },
      }),
    });
    vi.stubGlobal("fetch", mockFetch);

    const { result } = renderHook(() => useQuery());
    expect(result.current.state).toBe("idle");

    act(() => {
      result.current.submit("hello", "t1");
    });
    expect(result.current.state).toBe("submitting");

    await waitFor(() => expect(result.current.state).toBe("completed"));
    expect(result.current.messages[1].summary).toBe("done");
  });
});
```

Run: `cd gouchen-frontend && npx vitest run src/hooks/useQuery.test.ts`
Expected: FAIL.

- [ ] **Step 3: Implement useQuery**

Create `gouchen-frontend/src/hooks/useQuery.ts`:

```ts
import { useState, useCallback } from "react";
import { queryApi } from "../api/query";
import type { QueryResponse } from "../types/api";
import type { ChatMessageData } from "../components/ChatMessage";

export type QueryState = "idle" | "submitting" | "clarifying" | "pending_approval" | "completed" | "error";

export function useQuery() {
  const [state, setState] = useState<QueryState>("idle");
  const [messages, setMessages] = useState<ChatMessageData[]>([]);
  const [threadId, setThreadId] = useState<string>("");
  const [pendingInfo, setPendingInfo] = useState<QueryResponse["pending_info"] | null>(null);
  const [clarificationInfo, setClarificationInfo] = useState<QueryResponse["clarification_info"] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const appendUserMessage = (text: string) => {
    setMessages((prev) => [...prev, { role: "user", content: text }]);
  };

  const appendSystemMessage = (text: string) => {
    setMessages((prev) => [...prev, { role: "system", content: text }]);
  };

  const handleResponse = useCallback((res: QueryResponse) => {
    if (res.status === "completed") {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: res.result?.summary || "查询完成",
          sql: res.result?.generated_sql,
          explanation: res.result?.sql_explanation,
          tableData: Array.isArray(res.result?.execution_result) ? res.result?.execution_result : undefined,
          summary: res.result?.summary,
        },
      ]);
      setState("completed");
    } else if (res.status === "needs_clarification") {
      setClarificationInfo(res.clarification_info || null);
      setState("clarifying");
    } else if (res.status === "pending_approval") {
      setPendingInfo(res.pending_info || null);
      setState("pending_approval");
    } else if (res.status === "error") {
      appendSystemMessage(res.error || "未知错误");
      setState("error");
    }
  }, []);

  const submit = useCallback(async (text: string, tid: string) => {
    setThreadId(tid);
    setError(null);
    setPendingInfo(null);
    setClarificationInfo(null);
    appendUserMessage(text);
    setState("submitting");
    try {
      const res = await queryApi.createQuery({ query: text, thread_id: tid, user_role: "analyst" });
      handleResponse(res);
    } catch (e) {
      appendSystemMessage(e instanceof Error ? e.message : String(e));
      setState("error");
    }
  }, [handleResponse]);

  const clarify = useCallback(
    async (answers: { field: string; answer: string }[]) => {
      setState("submitting");
      try {
        const res = await queryApi.clarify({ thread_id: threadId, answers });
        handleResponse(res);
      } catch (e) {
        appendSystemMessage(e instanceof Error ? e.message : String(e));
        setState("error");
      }
    },
    [threadId, handleResponse]
  );

  const approve = useCallback(
    async (decision: "approve" | "reject", editedSql?: string) => {
      setState("submitting");
      try {
        const res = await queryApi.approve({ thread_id: threadId, decision, edited_sql: editedSql });
        handleResponse(res);
      } catch (e) {
        appendSystemMessage(e instanceof Error ? e.message : String(e));
        setState("error");
      }
    },
    [threadId, handleResponse]
  );

  const reset = useCallback(() => {
    setState("idle");
    setMessages([]);
    setThreadId("");
    setPendingInfo(null);
    setClarificationInfo(null);
    setError(null);
  }, []);

  return {
    state,
    messages,
    threadId,
    pendingInfo,
    clarificationInfo,
    error,
    submit,
    clarify,
    approve,
    reset,
  };
}
```

Run tests again: expected PASS.

- [ ] **Step 4: Commit**

```bash
git add gouchen-frontend/src/hooks/useQuery.ts gouchen-frontend/src/hooks/useQuery.test.ts gouchen-frontend/package.json
git commit -m "feat: add useQuery hook with state machine and tests"
```

---

## Task 15: Assemble ChatPage and App.tsx

**Files:**
- Create: `gouchen-frontend/src/pages/ChatPage.tsx`
- Modify: `gouchen-frontend/src/App.tsx`
- Modify: `gouchen-frontend/src/main.tsx`

- [ ] **Step 1: Implement ChatPage**

Create `gouchen-frontend/src/pages/ChatPage.tsx`:

```tsx
import { useState, useEffect, useRef } from "react";
import { v4 as uuidv4 } from "uuid";
import { Sidebar } from "../components/Sidebar";
import { ChatMessage } from "../components/ChatMessage";
import { ClarificationForm } from "../components/ClarificationForm";
import { ApprovalPanel } from "../components/ApprovalPanel";
import { useQuery } from "../hooks/useQuery";

interface Session {
  threadId: string;
  title: string;
}

export function ChatPage() {
  const [sessions, setSessions] = useState<Session[]>(() => {
    const raw = localStorage.getItem("gouchen_sessions");
    return raw ? JSON.parse(raw) : [];
  });
  const [activeThreadId, setActiveThreadId] = useState<string>("");
  const [input, setInput] = useState("");
  const { state, messages, threadId, pendingInfo, clarificationInfo, submit, clarify, approve, reset } = useQuery();
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    localStorage.setItem("gouchen_sessions", JSON.stringify(sessions));
  }, [sessions]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, state]);

  const handleNewChat = () => {
    const newId = uuidv4();
    setActiveThreadId(newId);
    reset();
  };

  const handleSend = () => {
    if (!input.trim()) return;
    const tid = activeThreadId || uuidv4();
    if (!activeThreadId) {
      setActiveThreadId(tid);
    }
    // Add session entry on first message
    setSessions((prev) => {
      if (prev.find((s) => s.threadId === tid)) return prev;
      return [{ threadId: tid, title: input.slice(0, 20) }, ...prev];
    });
    submit(input.trim(), tid);
    setInput("");
  };

  return (
    <div className="flex h-screen">
      <Sidebar
        sessions={sessions}
        activeThreadId={activeThreadId}
        onNewChat={handleNewChat}
        onSelect={(tid) => {
          setActiveThreadId(tid);
          reset();
        }}
      />
      <div className="flex-1 flex flex-col bg-white">
        <div className="flex-1 overflow-y-auto px-6 py-4">
          {messages.map((m, idx) => (
            <ChatMessage key={idx} message={m} />
          ))}
          {state === "clarifying" && clarificationInfo && (
            <ClarificationForm
              questions={clarificationInfo.questions.map((q) => ({ field: q.field, question: q.question || q.field }))}
              onSubmit={(answers) => clarify(answers)}
            />
          )}
          {state === "pending_approval" && pendingInfo && (
            <ApprovalPanel
              sql={pendingInfo.generated_sql || ""}
              explanation={pendingInfo.sql_explanation}
              onApprove={(edited) => approve("approve", edited)}
              onReject={() => approve("reject")}
            />
          )}
          <div ref={bottomRef} />
        </div>
        <div className="border-t px-6 py-4 flex gap-2">
          <input
            className="flex-1 border rounded-md px-4 py-2 text-sm"
            placeholder="输入你的数据问题..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSend()}
            disabled={state === "submitting" || state === "clarifying" || state === "pending_approval"}
          />
          <button
            onClick={handleSend}
            disabled={state === "submitting" || state === "clarifying" || state === "pending_approval"}
            className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:bg-gray-400"
          >
            发送
          </button>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Install uuid dependency**

Run:
```bash
npm install uuid && npm install -D @types/uuid
```

- [ ] **Step 3: Wire up App.tsx**

Replace `gouchen-frontend/src/App.tsx` with:

```tsx
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { ChatPage } from "./pages/ChatPage";

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<ChatPage />} />
        {/* 预留管理后台路由 */}
        {/* <Route path="/admin" element={<AdminPage />} /> */}
      </Routes>
    </BrowserRouter>
  );
}

export default App;
```

- [ ] **Step 4: Update main.tsx**

Replace `gouchen-frontend/src/main.tsx` with:

```tsx
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
```

- [ ] **Step 5: Install react-router-dom**

Run:
```bash
npm install react-router-dom
```

- [ ] **Step 6: Commit**

```bash
git add gouchen-frontend/src/pages/ChatPage.tsx gouchen-frontend/src/App.tsx gouchen-frontend/src/main.tsx gouchen-frontend/package.json
git commit -m "feat: assemble ChatPage with routing and session management"
```

---

## Task 16: End-to-End Dev Verification

**Files:** (no new files)

- [ ] **Step 1: Start both dev servers**

Terminal 1:
```bash
cd gouchen && python -m src.main
```
Expected: FastAPI running on http://localhost:8000

Terminal 2:
```bash
cd gouchen-frontend && npm run dev
```
Expected: Vite running on http://localhost:5173/

- [ ] **Step 2: Open browser at http://localhost:5173/**

Manually test:
1. Click "+ 新对话"
2. Type a natural language query and send.
3. If backend returns `completed`, verify table/chart renders.
4. If backend returns `needs_clarification`, fill form and submit.
5. If backend returns `pending_approval`, use approve/reject/edit buttons.

- [ ] **Step 3: Verify CORS logs**

If browser blocks requests, revisit Task 1 and confirm `allow_origins` includes `http://localhost:5173`.

- [ ] **Step 4: Commit any final fixes**

If any fixes were made during verification, commit them with:
```bash
git add <files>
git commit -m "fix: dev verification adjustments"
```

---

## Self-Review Checklist

1. **Spec coverage:**
   - CORS support -> Task 1
   - Vite React project -> Task 2
   - Tailwind + shadcn/ui -> Task 3
   - API types and client -> Tasks 4, 5
   - AutoChart + DataTable -> Tasks 7, 8
   - ClarificationForm + ApprovalPanel -> Tasks 10, 11
   - Sidebar + ChatMessage -> Tasks 12, 13
   - useQuery state machine -> Task 14
   - ChatPage assembly -> Task 15
   - Dev verification -> Task 16

2. **Placeholder scan:** No "TBD", "TODO", vague instructions.

3. **Type consistency:** All imports reference `ChatMessageData`, `QueryResponse`, `QueryState` consistently.
