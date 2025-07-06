import { run } from '@proven-network/handler';
import { Todo, CreateTodoRequest, UpdateTodoRequest, TodoFilter } from './types';
import { getCurrentIdentity } from '@proven-network/session';

// In-memory storage for this example (in a real app, this would be persistent storage)
let todos: Todo[] = [];
let nextId = 1;

// Non-handler utility function that can be imported directly
export const formatTodoId = (id: number): string => `todo-${id}`;

// Non-handler constant that can be imported directly
export const MAX_TODOS = 100;

/**
 * Create a new todo item
 */
export const createTodo = run((request: CreateTodoRequest): Todo => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  console.log('Creating new todo:', request);

  const now = new Date();
  const todo: Todo = {
    id: formatTodoId(nextId++),
    title: request.title,
    description: request.description,
    completed: false,
    createdAt: now,
    updatedAt: now,
  };

  todos.push(todo);
  console.log(`Created todo "${todo.title}" with ID: ${todo.id}`);

  return todo;
});

/**
 * Get all todos with optional filtering
 */
export const getTodos = run((filter?: TodoFilter): Todo[] => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  console.log('Fetching todos with filter:', filter);

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
export const updateTodo = run((request: UpdateTodoRequest): Todo => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  console.log('Updating todo:', request);

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
  console.log(`Updated todo "${updatedTodo.title}"`);

  return updatedTodo;
});

/**
 * Delete a todo by ID
 */
export const deleteTodo = run((todoId: string): boolean => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  console.log('Deleting todo:', todoId);

  const initialLength = todos.length;
  todos = todos.filter((todo) => todo.id !== todoId);

  const deleted = todos.length < initialLength;
  if (deleted) {
    console.log(`Deleted todo with ID: ${todoId}`);
  } else {
    console.log(`Todo with ID ${todoId} not found`);
  }

  return deleted;
});

/**
 * Mark all todos as completed or uncompleted
 */
export const toggleAllTodos = run((completed: boolean): Todo[] => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

  console.log(`Marking all todos as ${completed ? 'completed' : 'uncompleted'}`);

  const now = new Date();
  todos = todos.map((todo) => ({
    ...todo,
    completed,
    updatedAt: now,
  }));

  console.log(`Updated ${todos.length} todos`);
  return todos;
});

/**
 * Get todo statistics
 */
export const getTodoStats = run(() => {
  const identity = getCurrentIdentity();

  if (!identity) {
    throw new Error('User is not authenticated');
  }

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
