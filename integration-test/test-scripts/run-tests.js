const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

/**
 * Main test runner script
 */
function runTests() {
  console.log('=====================================');
  console.log('Starting Proven Node Integration Tests');
  console.log('=====================================');

  // Get all test scripts (excluding this runner)
  const testDir = '/app';
  const testFiles = fs.readdirSync(testDir)
    .filter(file => file.startsWith('test-') && file.endsWith('.js') && file !== 'run-tests.js')
    .map(file => path.join(testDir, file));

  if (testFiles.length === 0) {
    console.error('No test scripts found!');
    return 1;
  }

  // Run each test script
  let allPassed = true;
  
  for (const testScript of testFiles) {
    console.log(`\nRunning test: ${testScript}`);
    
    try {
      // Execute the test script
      execSync(`node ${testScript}`, { stdio: 'inherit' });
      console.log(`✅ Test passed: ${testScript}`);
    } catch (error) {
      console.error(`❌ Test failed: ${testScript}`);
      allPassed = false;
    }
    
    console.log('');
  }

  if (allPassed) {
    console.log('All tests passed! ✅');
    return 0;
  } else {
    console.error('Some tests failed ❌');
    return 1;
  }
}

// Run the tests and exit with the appropriate code
process.exit(runTests()); 
