import { run } from '@proven-network/handler';
import { Todo, CreateTodoRequest, UpdateTodoRequest, TodoFilter } from './types';
import { getCurrentIdentity } from '@proven-network/session';
import { getApplicationStore, StringStore } from '@proven-network/kv';

// Use KV store for persistent storage
const todoStore: StringStore = getApplicationStore('todos');
const TODOS_KEY = 'all_todos';

// Non-handler utility function that can be imported directly
export const formatTodoId = (id: string): string => `todo-${id}`;

// Non-handler constant that can be imported directly
export const MAX_TODOS = 100;

/**
 * Create a new todo item
 */
export const createTodo = run(async (request: CreateTodoRequest): Promise<Todo> => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  console.log('Creating new todo:', request);

  // Generate UUID using crypto.randomUUID()
  const uuid = crypto.randomUUID();
  const now = new Date();
  const todo: Todo = {
    id: formatTodoId(uuid),
    title: request.title,
    description: request.description,
    completed: false,
    createdAt: now,
    updatedAt: now,
  };

  // Get current todos from store
  const todosJson = await todoStore.get(TODOS_KEY);
  const todos: Todo[] = todosJson ? JSON.parse(todosJson) : [];

  // Add new todo and save back to store
  todos.push(todo);
  await todoStore.set(TODOS_KEY, JSON.stringify(todos));

  console.log(`Created todo "${todo.title}" with ID: ${todo.id}`);

  return todo;
});

/**
 * Get all todos with optional filtering
 */
export const getTodos = run(async (filter?: TodoFilter): Promise<Todo[]> => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  console.log('Fetching todos with filter:', filter);

  // Get todos from store
  const todosJson = await todoStore.get(TODOS_KEY);
  const todos: Todo[] = todosJson ? JSON.parse(todosJson) : [];

  let filteredTodos = [...todos];

  if (filter?.completed !== undefined) {
    filteredTodos = filteredTodos.filter((todo) => todo.completed === filter.completed);
  }

  if (filter?.search) {
    const searchLower = filter.search.toLowerCase();
    filteredTodos = filteredTodos.filter(
      (todo) =>
        todo.title.toLowerCase().includes(searchLower) ||
        todo.description?.toLowerCase().includes(searchLower)
    );
  }

  console.log(`Returning ${filteredTodos.length} todos`);
  return filteredTodos;
});

/**
 * Update an existing todo
 */
export const updateTodo = run(async (request: UpdateTodoRequest): Promise<Todo> => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  console.log('Updating todo:', request);

  // Get todos from store
  const todosJson = await todoStore.get(TODOS_KEY);
  const todos: Todo[] = todosJson ? JSON.parse(todosJson) : [];

  const todoIndex = todos.findIndex((todo) => todo.id === request.id);
  if (todoIndex === -1) {
    throw new Error(`Todo with ID ${request.id} not found`);
  }

  const todo = todos[todoIndex];
  const updatedTodo: Todo = {
    ...todo,
    ...request,
    updatedAt: new Date(),
  };

  todos[todoIndex] = updatedTodo;

  // Save updated todos back to store
  await todoStore.set(TODOS_KEY, JSON.stringify(todos));

  console.log(`Updated todo "${updatedTodo.title}"`);

  return updatedTodo;
});

/**
 * Delete a todo by ID
 */
export const deleteTodo = run(async (todoId: string): Promise<boolean> => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  console.log('Deleting todo:', todoId);

  // Get todos from store
  const todosJson = await todoStore.get(TODOS_KEY);
  const todos: Todo[] = todosJson ? JSON.parse(todosJson) : [];

  const initialLength = todos.length;
  const filteredTodos = todos.filter((todo) => todo.id !== todoId);

  const deleted = filteredTodos.length < initialLength;
  if (deleted) {
    // Save updated todos back to store
    await todoStore.set(TODOS_KEY, JSON.stringify(filteredTodos));
    console.log(`Deleted todo with ID: ${todoId}`);
  } else {
    console.log(`Todo with ID ${todoId} not found`);
  }

  return deleted;
});

/**
 * Mark all todos as completed or uncompleted
 */
export const toggleAllTodos = run(async (completed: boolean): Promise<Todo[]> => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  console.log(`Marking all todos as ${completed ? 'completed' : 'uncompleted'}`);

  // Get todos from store
  const todosJson = await todoStore.get(TODOS_KEY);
  const todos: Todo[] = todosJson ? JSON.parse(todosJson) : [];

  const now = new Date();
  const updatedTodos = todos.map((todo) => ({
    ...todo,
    completed,
    updatedAt: now,
  }));

  // Save updated todos back to store
  await todoStore.set(TODOS_KEY, JSON.stringify(updatedTodos));

  console.log(`Updated ${updatedTodos.length} todos`);
  return updatedTodos;
});

/**
 * Get todo statistics
 */
export const getTodoStats = run(async () => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  // Get todos from store
  const todosJson = await todoStore.get(TODOS_KEY);
  const todos: Todo[] = todosJson ? JSON.parse(todosJson) : [];

  const total = todos.length;
  const completed = todos.filter((todo) => todo.completed).length;
  const pending = total - completed;

  const stats = {
    total,
    completed,
    pending,
    completionRate: total > 0 ? Math.round((completed / total) * 100) : 0,
  };

  console.log('Todo statistics:', stats);
  return stats;
});
