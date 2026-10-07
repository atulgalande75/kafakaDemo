import { useAuth } from './auth/AuthProvider';
import { ActivityFeed } from './components/ActivityFeed';
import { FlagsLine } from './components/FlagsLine';
import { Header } from './components/Header';
import { OrderForm } from './components/OrderForm';
import { OrdersTable } from './components/OrdersTable';
import { StockPanel } from './components/StockPanel';
import { LiveProvider } from './live/LiveProvider';

function Dashboard() {
  return (
    <>
      <Header />
      <main className="grid">
        <div className="col">
          <StockPanel />
          <OrderForm />
          <OrdersTable />
        </div>
        <div className="col">
          <ActivityFeed />
        </div>
      </main>
      <FlagsLine />
    </>
  );
}

export function App() {
  const { status, error, login } = useAuth();

  if (status === 'loading') return <p className="splash">Signing you in…</p>;

  if (status === 'error') {
    return (
      <div className="splash">
        <h1>Sign-in failed</h1>
        <p className="error">{error}</p>
        <button type="button" onClick={() => void login()}>
          Try again
        </button>
      </div>
    );
  }

  if (status === 'unauthenticated') {
    return (
      <div className="splash">
        <h1>Orderflow</h1>
        <p className="muted">Live stock and orders, straight from Kafka.</p>
        <button type="button" onClick={() => void login()}>
          Sign in
        </button>
        <p className="hint">Demo users: alice / alice, bob / bob</p>
      </div>
    );
  }

  return (
    <LiveProvider>
      <Dashboard />
    </LiveProvider>
  );
}
