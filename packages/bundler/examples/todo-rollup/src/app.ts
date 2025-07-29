// Example of how the app should work with bundler transformation
import { sdk } from './sdk-init';
import { effect } from '@preact/signals-core';

// Import handlers directly - bundler will transform these
import { createTodo, getTodos, updateTodo, deleteTodo, getTodoStats } from './todo-handlers';

// App state
let currentFilter: 'all' | 'pending' | 'completed' = 'all';
let currentSearch = '';

export async function initApp() {
  try {
    console.log('🎯 Initializing Rollup Todo App...');

    // Initialize auth button
    await sdk.initConnectButton('#auth-container');
    console.log('Auth button initialized');

    // Wait a bit for the bridge to be ready and signals to be initialized
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Debug: Check initial signal values
    console.log('🔍 Initial signal values:', {
      authState: sdk.authState.value,
      userInfo: sdk.userInfo.value,
      isAuthenticated: sdk.isAuthenticated.value,
    });

    // Debug: Check signal objects
    console.log('🔍 Signal objects:', {
      authState: sdk.authState,
      userInfo: sdk.userInfo,
      isAuthenticated: sdk.isAuthenticated,
    });

    // Debug: Check if signals are reactive
    console.log('🔍 Signal reactivity test:');
    console.log('  authState type:', typeof sdk.authState);
    console.log('  authState.value:', sdk.authState.value);
    console.log('  isAuthenticated type:', typeof sdk.isAuthenticated);
    console.log('  isAuthenticated.value:', sdk.isAuthenticated.value);

    // Debug: Test signal subscription directly
    console.log('🔍 Testing direct signal subscription...');
    sdk.authState.subscribe((value: any) => {
      console.log('📡 Direct authState subscription fired:', value);
    });
    sdk.isAuthenticated.subscribe((value: any) => {
      console.log('📡 Direct isAuthenticated subscription fired:', value);
      // Manually update UI when authentication state changes
      updateAuthUI(value);
      if (value) {
        loadTodos().catch(console.error);
        updateStats().catch(console.error);
      }
    });

    // Set up reactive authentication state handling
    setupReactiveAuth();

    console.log('✅ Rollup Todo App initialized successfully');
  } catch (error) {
    console.error('Failed to initialize app:', error);
  }
}

// Set up reactive authentication state handling
function setupReactiveAuth() {
  console.log('🔄 Setting up reactive authentication state...');

  // React to authentication state changes
  // Try accessing signals in a different way to ensure proper tracking
  const mainEffect = effect(() => {
    console.log('🔄 Main effect running...');

    // Try to force reactivity by accessing the signal objects first
    const authStateSignal = sdk.authState;
    const userInfoSignal = sdk.userInfo;
    const isAuthenticatedSignal = sdk.isAuthenticated;

    // Then access their values
    const authState = authStateSignal.value;
    const userInfo = userInfoSignal.value;
    const isAuthenticated = isAuthenticatedSignal.value;

    console.log('🔄 Main effect - current values:', { authState, isAuthenticated, userInfo });

    // Update UI based on authentication state
    updateAuthUI(isAuthenticated);

    // Load todos and stats when user is authenticated
    if (isAuthenticated) {
      console.log('🔄 User is authenticated, loading todos and stats...');
      loadTodos().catch(console.error);
      updateStats().catch(console.error);
    }
  });

  console.log('🔄 Main effect created:', mainEffect);

  // Also set up individual effects for better debugging
  const authStateEffect = effect(() => {
    const authState = sdk.authState.value;
    console.log('🔄 Auth state signal changed:', authState);
  });

  const isAuthenticatedEffect = effect(() => {
    const isAuthenticated = sdk.isAuthenticated.value;
    console.log('🔄 Is authenticated signal changed:', isAuthenticated);
  });

  const userInfoEffect = effect(() => {
    const userInfo = sdk.userInfo.value;
    console.log('🔄 User info signal changed:', userInfo);
  });

  console.log('🔄 Individual effects created:', {
    authStateEffect,
    isAuthenticatedEffect,
    userInfoEffect,
  });

  // Debug: Print auth signal status every 5 seconds
  setInterval(() => {
    const authState = sdk.authState.value;
    const userInfo = sdk.userInfo.value;
    const isAuthenticated = sdk.isAuthenticated.value;

    console.log('🔄 Auth Status Check (every 5s):', {
      authState,
      isAuthenticated,
      userInfo,
      timestamp: new Date().toISOString(),
    });
  }, 5000);

  // Debug: Add manual signal testing
  (window as any).testSignals = () => {
    console.log('🧪 Testing signal reactivity...');
    console.log('Current values:', {
      authState: sdk.authState.value,
      userInfo: sdk.userInfo.value,
      isAuthenticated: sdk.isAuthenticated.value,
    });
  };

  // Debug: Add manual signal change testing
  (window as any).testSignalChange = () => {
    console.log('🧪 Manually changing auth state for testing...');
    // This will only work if the signals are properly exposed
    try {
      // Try to manually trigger a signal change (for testing only)
      console.log('Attempting to manually change auth state...');
      // Note: This is just for testing - in real usage, the bridge should update the signals
    } catch (error) {
      console.error('Error testing signal change:', error);
    }
  };
}

export async function addTodo(title: string, description?: string) {
  if (!title.trim()) return;

  try {
    // Direct function call - bundler transformation handles the SDK communication
    const newTodo = await createTodo({
      title: title.trim(),
      description: description?.trim(),
    });

    console.log('Created todo:', newTodo);

    // Refresh data
    await loadTodos();
    await updateStats();
  } catch (error) {
    console.error('Error creating todo:', error);
    throw error;
  }
}

export async function loadTodos() {
  try {
    // Build filter
    const filter: any = {};
    if (currentFilter === 'completed') filter.completed = true;
    if (currentFilter === 'pending') filter.completed = false;
    if (currentSearch) filter.search = currentSearch;

    // Direct function call - bundler transformation handles the SDK communication
    const todos = await getTodos(filter);
    renderTodos(todos);
  } catch (error) {
    console.error('Error loading todos:', error);
    renderError('Failed to load todos');
  }
}

export async function toggleTodo(id: string, completed: boolean) {
  try {
    // Direct function call - bundler transformation handles the SDK communication
    await updateTodo({
      id,
      completed,
    });

    await loadTodos();
    await updateStats();
  } catch (error) {
    console.error('Error updating todo:', error);
    throw error;
  }
}

export async function deleteTodoItem(id: string) {
  if (!confirm('Are you sure you want to delete this todo?')) return;

  try {
    // Direct function call - bundler transformation handles the SDK communication
    await deleteTodo(id);

    await loadTodos();
    await updateStats();
  } catch (error) {
    console.error('Error deleting todo:', error);
    throw error;
  }
}

export async function updateStats() {
  try {
    // Direct function call - bundler transformation handles the SDK communication
    const stats = await getTodoStats();

    updateStatsUI(stats);
  } catch (error) {
    console.error('Error updating stats:', error);
  }
}

export function setFilter(filter: 'all' | 'pending' | 'completed') {
  currentFilter = filter;
  loadTodos();
}

export function setSearch(search: string) {
  currentSearch = search;
  loadTodos();
}

// UI update functions (would be in a separate UI module in a real app)
function updateAuthUI(isAuthenticated: boolean) {
  console.log('🎨 updateAuthUI called with isAuthenticated:', isAuthenticated);

  const authStatus = document.getElementById('auth-status');
  const authText = document.getElementById('auth-text');
  const todoInterface = document.getElementById('todo-interface');
  const signinPrompt = document.getElementById('signin-prompt');

  console.log('🎨 DOM elements found:', {
    authStatus: !!authStatus,
    authText: !!authText,
    todoInterface: !!todoInterface,
    signinPrompt: !!signinPrompt,
  });

  if (!authStatus || !authText || !todoInterface || !signinPrompt) {
    console.error('🎨 updateAuthUI: Missing DOM elements, returning early');
    return;
  }

  if (isAuthenticated) {
    // User is authenticated - show todo interface
    console.log('🎨 User is authenticated, showing todo interface');
    authStatus.className = 'auth-status authenticated';
    authText.textContent = '✅ Authenticated - Managing your private todos';

    // Show todo interface and hide signin prompt
    todoInterface.style.display = 'block';
    signinPrompt.style.display = 'none';

    console.log('🎨 Updated DOM elements:', {
      authStatusClass: authStatus.className,
      authTextContent: authText.textContent,
      todoInterfaceDisplay: todoInterface.style.display,
      signinPromptDisplay: signinPrompt.style.display,
    });
  } else {
    // User is not authenticated - show signin prompt
    console.log('🎨 User is not authenticated, showing signin prompt');
    authStatus.className = 'auth-status unauthenticated';
    authText.textContent = '❌ Not authenticated - Please sign in to access your todos';

    // Hide todo interface and show signin prompt
    todoInterface.style.display = 'none';
    signinPrompt.style.display = 'block';

    console.log('🎨 Updated DOM elements:', {
      authStatusClass: authStatus.className,
      authTextContent: authText.textContent,
      todoInterfaceDisplay: todoInterface.style.display,
      signinPromptDisplay: signinPrompt.style.display,
    });
  }
}

function renderTodos(todos: any[]) {
  const container = document.getElementById('todos-container');
  if (!container) return;

  if (todos.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <svg width="64" height="64" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M9 11L12 14L22 4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M21 12V19C21 19.5304 20.7893 20.0391 20.4142 20.4142C20.0391 20.7893 19.5304 21 19 21H5C4.46957 21 3.96086 20.7893 3.58579 20.4142C3.21071 20.0391 3 19.5304 3 19V5C3 4.46957 3.21071 3.96086 3.58579 3.58579C3.96086 3.21071 4.46957 3 5 3H16" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <p>No todos found</p>
      </div>`;
    return;
  }

  container.innerHTML = todos
    .map(
      (todo) => `
      <div class="todo-item ${todo.completed ? 'completed' : ''}" onclick="window.todoApp.toggleTodo('${todo.id}', ${!todo.completed})">
        <div class="todo-checkbox">
          <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M20 6L9 17L4 12" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </div>
        <div class="todo-content">
          <div class="todo-title">${escapeHtml(todo.title)}</div>
          ${todo.description ? `<div class="todo-description">${escapeHtml(todo.description)}</div>` : ''}
          <div class="todo-meta">
            ${new Date(todo.createdAt).toLocaleDateString('en-US', {
              month: 'short',
              day: 'numeric',
              year: 'numeric',
            })}
          </div>
        </div>
        <div class="todo-actions">
          <button onclick="event.stopPropagation(); window.todoApp.deleteTodoItem('${todo.id}')" title="Delete todo">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M3 6H5H21" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
              <path d="M8 6V4C8 3.46957 8.21071 2.96086 8.58579 2.58579C8.96086 2.21071 9.46957 2 10 2H14C14.5304 2 15.0391 2.21071 15.4142 2.58579C15.7893 2.96086 16 3.46957 16 4V6M19 6V20C19 20.5304 18.7893 21.0391 18.4142 21.4142C18.0391 21.7893 17.5304 22 17 22H7C6.46957 22 5.96086 21.7893 5.58579 21.4142C5.21071 21.0391 5 20.5304 5 20V6H19Z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
          </button>
        </div>
      </div>
    `
    )
    .join('');
}

function renderError(message: string) {
  const container = document.getElementById('todos-container');
  if (!container) return;

  container.innerHTML = `<p style="text-align: center; color: #dc3545; padding: 20px;">${message}</p>`;
}

function updateStatsUI(stats: any) {
  const elements = {
    total: document.getElementById('total-count'),
    completed: document.getElementById('completed-count'),
    pending: document.getElementById('pending-count'),
    rate: document.getElementById('completion-rate'),
  };

  if (elements.total) elements.total.textContent = stats.total;
  if (elements.completed) elements.completed.textContent = stats.completed;
  if (elements.pending) elements.pending.textContent = stats.pending;
  if (elements.rate) elements.rate.textContent = stats.completionRate + '%';
}

function escapeHtml(text: string): string {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// HTML form handler functions
export function handleAddTodoForm(event: Event) {
  event.preventDefault();

  const titleElement = document.getElementById('todo-title') as HTMLInputElement;
  const descriptionElement = document.getElementById('todo-description') as HTMLTextAreaElement;

  if (!titleElement || !descriptionElement) {
    console.error('Form elements not found');
    return;
  }

  const title = titleElement.value;
  const description = descriptionElement.value;

  if (!title.trim()) return;

  addTodo(title, description)
    .then(() => {
      // Clear form
      titleElement.value = '';
      descriptionElement.value = '';
    })
    .catch((error) => {
      console.error('Error creating todo:', error);
      alert('Failed to create todo: ' + error.message);
    });
}

export function handleFilterTodos(filter: 'all' | 'pending' | 'completed') {
  setFilter(filter);

  // Update button states
  document.querySelectorAll('.filter-btn').forEach((btn) => btn.classList.remove('active'));
  const filterButton = document.getElementById('filter-' + filter);
  if (filterButton) {
    filterButton.classList.add('active');
  }
}

export function handleSearchTodos(query: string) {
  setSearch(query);
}

// Set up global functions for HTML onclick handlers
function setupGlobalHandlers() {
  (window as any).addTodo = handleAddTodoForm;
  (window as any).filterTodos = handleFilterTodos;
  (window as any).searchTodos = handleSearchTodos;
}

// Export functions for global access (temporary until we have proper event binding)
(window as any).todoApp = {
  addTodo,
  toggleTodo,
  deleteTodoItem,
  setFilter,
  setSearch,
};

// Initialize global handlers when module loads
setupGlobalHandlers();
