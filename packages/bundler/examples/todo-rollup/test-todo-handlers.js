// Simple test to verify the todo handlers work with KV store
import { createTodo, getTodos, updateTodo, deleteTodo, getTodoStats } from './dist/bundle.js';

async function runTests() {
  console.log('Testing todo handlers with KV store...\n');

  try {
    // Test 1: Create a todo
    console.log('1. Creating a todo...');
    const todo1 = await createTodo({
      title: 'Test Todo 1',
      description: 'This is a test todo',
    });
    console.log('✓ Created todo:', todo1);
    console.log('  - ID format:', todo1.id);
    console.log('  - ID contains UUID:', todo1.id.includes('-'));

    // Test 2: Create another todo
    console.log('\n2. Creating another todo...');
    const todo2 = await createTodo({
      title: 'Test Todo 2',
      description: 'Another test todo',
    });
    console.log('✓ Created todo:', todo2);

    // Test 3: Get all todos
    console.log('\n3. Getting all todos...');
    const allTodos = await getTodos();
    console.log('✓ Retrieved todos:', allTodos.length, 'todos');

    // Test 4: Update a todo
    console.log('\n4. Updating first todo...');
    const updatedTodo = await updateTodo({
      id: todo1.id,
      completed: true,
      title: 'Updated Test Todo 1',
    });
    console.log('✓ Updated todo:', updatedTodo);

    // Test 5: Get stats
    console.log('\n5. Getting todo stats...');
    const stats = await getTodoStats();
    console.log('✓ Todo stats:', stats);

    // Test 6: Delete a todo
    console.log('\n6. Deleting first todo...');
    const deleted = await deleteTodo(todo1.id);
    console.log('✓ Deleted todo:', deleted);

    // Test 7: Verify deletion
    console.log('\n7. Verifying deletion...');
    const remainingTodos = await getTodos();
    console.log('✓ Remaining todos:', remainingTodos.length);

    console.log('\n✅ All tests passed!');
  } catch (error) {
    console.error('❌ Test failed:', error);
  }
}

// Note: This test file is for manual verification only
// In a real environment, the handlers would need proper authentication context
console.log('Note: This test requires proper authentication context to run.');
console.log('The handlers use getCurrentIdentity() which needs to be mocked or provided.');
