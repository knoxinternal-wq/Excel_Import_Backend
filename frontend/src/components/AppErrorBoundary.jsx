import { Component } from 'react';

export default class AppErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, errorMessage: '' };
  }

  static getDerivedStateFromError(error) {
    return {
      hasError: true,
      errorMessage: error?.message ? String(error.message) : 'Unexpected UI error',
    };
  }

  componentDidCatch() {
    // Keep this intentionally minimal: prevent a blank screen in production.
  }

  handleReload = () => {
    window.location.reload();
  };

  render() {
    if (!this.state.hasError) return this.props.children;

    return (
      <div className="min-h-screen bg-slate-50 flex items-center justify-center p-6">
        <div className="w-full max-w-xl bg-white border border-red-200 rounded-xl shadow-sm p-6">
          <h1 className="text-lg font-semibold text-red-700">Something went wrong</h1>
          <p className="mt-2 text-sm text-slate-700">
            The page hit an unexpected error. Your app did not close; you can reload safely.
          </p>
          <p className="mt-2 text-xs text-slate-500 break-all">
            {this.state.errorMessage}
          </p>
          <button
            type="button"
            onClick={this.handleReload}
            className="mt-4 inline-flex items-center px-4 py-2 text-sm font-medium text-white bg-indigo-600 rounded-lg hover:bg-indigo-700"
          >
            Reload app
          </button>
        </div>
      </div>
    );
  }
}
