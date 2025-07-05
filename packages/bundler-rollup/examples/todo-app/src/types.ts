// Type definitions for the todo app

export interface Todo {
  id: string;
  title: string;
  description?: string;
  completed: boolean;
  createdAt: Date;
  updatedAt: Date;
}

export interface CreateTodoRequest {
  title: string;
  description?: string;
}

export interface UpdateTodoRequest {
  id: string;
  title?: string;
  description?: string;
  completed?: boolean;
}

export interface TodoFilter {
  completed?: boolean;
  search?: string;
}
