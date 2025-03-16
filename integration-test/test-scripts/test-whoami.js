const fetch = require('node-fetch');

/**
 * Tests the /whoami endpoint for all nodes
 */
async function testWhoami() {
  console.log('Testing /whoami endpoint for all nodes...');

  // Define node information with expected specializations
  const nodes = {
    'bulbasaur.local:3201': 'RadixMainnet',
    'charmander.local:3202': 'RadixStokenet',
    'squirtle.local:3203': 'none'
  };

  let allTestsPassed = true;

  for (const [nodeUrl, expectedSpecialization] of Object.entries(nodes)) {
    console.log(`\nTesting node: ${nodeUrl} (Expected specialization: ${expectedSpecialization})`);
    
    try {
      // Get the /whoami response
      const response = await fetch(`http://${nodeUrl}/whoami`);
      
      if (!response.ok) {
        throw new Error(`Failed to connect to node ${nodeUrl}: ${response.status} ${response.statusText}`);
      }
      
      const data = await response.json();
      
      // Extract node info
      const { fqdn, public_key, region, availability_zone, specializations } = data.node;
      
      console.log('Node info:');
      console.log(`  FQDN: ${fqdn}`);
      console.log(`  Public Key: ${public_key}`);
      console.log(`  Region: ${region}`);
      console.log(`  Availability Zone: ${availability_zone}`);
      console.log(`  Specializations: ${JSON.stringify(specializations)}`);
      
      // Validate FQDN matches the hostname used for connection
      const nodeHostname = nodeUrl.split(':')[0];
      if (fqdn !== nodeHostname) {
        console.error(`❌ FQDN mismatch. Expected: ${nodeHostname}, Got: ${fqdn}`);
        allTestsPassed = false;
        continue;
      }
      
      // Validate specializations
      if (expectedSpecialization === 'RadixMainnet') {
        if (!specializations.includes('RadixMainnet')) {
          console.error('❌ Node should have RadixMainnet specialization');
          allTestsPassed = false;
          continue;
        }
      } else if (expectedSpecialization === 'RadixStokenet') {
        if (!specializations.includes('RadixStokenet')) {
          console.error('❌ Node should have RadixStokenet specialization');
          allTestsPassed = false;
          continue;
        }
      } else if (expectedSpecialization === 'none') {
        if (specializations.length !== 0) {
          console.error('❌ Node should have no specializations');
          allTestsPassed = false;
          continue;
        }
      }
      
      console.log(`✅ Node ${nodeUrl} validated successfully`);
    } catch (error) {
      console.error(`❌ Error testing node ${nodeUrl}: ${error.message}`);
      allTestsPassed = false;
    }
  }

  if (allTestsPassed) {
    console.log('\n✅ All /whoami endpoint tests passed!');
    return 0; // Success exit code
  } else {
    console.error('\n❌ Some tests failed');
    return 1; // Failure exit code
  }
}

// Run the test and exit with the appropriate code
testWhoami()
  .then(exitCode => process.exit(exitCode))
  .catch(error => {
    console.error('Unhandled error:', error);
    process.exit(1);
  }); 
