import fluree from '../../out/fluree-node-sdk.js';

async function runDemo() {
    console.log('🚀 Fluree Node.js SDK Demo\n');
    
    try {
        // Step 1: Create a memory connection
        console.log('1️⃣ Creating memory connection...');
        const connection = await fluree.connectMemory({});
        console.log('✅ Connection created successfully!\n');
        
        // Step 2: Create a new ledger
        console.log('2️⃣ Creating ledger "demo-ledger"...');
        const ledger = await fluree.create(connection, 'demo-ledger');
        console.log('✅ Ledger created successfully!\n');
        
        // Step 3: Get initial database
        console.log('3️⃣ Getting initial database...');
        let db = await fluree.db(ledger);
        console.log('✅ Database ready!\n');
        
        // Step 4: Insert JSON-LD data
        console.log('4️⃣ Inserting JSON-LD data...');
        const jsonldData = {
            '@context': {
                'schema': 'http://schema.org/',
                'ex': 'http://example.org/'
            },
            'insert': [
                {
                    '@id': 'ex:person1',
                    '@type': 'schema:Person',
                    'schema:name': 'Alice Johnson',
                    'schema:age': 30,
                    'schema:email': 'alice@example.com',
                    'ex:department': 'Engineering',
                    'ex:skills': ['JavaScript', 'Python', 'Machine Learning']
                },
                {
                    '@id': 'ex:person2',
                    '@type': 'schema:Person',
                    'schema:name': 'Bob Smith',
                    'schema:age': 25,
                    'schema:email': 'bob@example.com',
                    'ex:department': 'Design',
                    'ex:skills': ['UI/UX', 'Photoshop', 'Figma']
                },
                {
                    '@id': 'ex:company',
                    '@type': 'schema:Organization',
                    'schema:name': 'Tech Corp',
                    'schema:url': 'https://techcorp.com',
                    'ex:employees': [
                        {'@id': 'ex:person1'},
                        {'@id': 'ex:person2'}
                    ]
                }
            ]
        };
        
        const stagedDb = await fluree.stage(db, jsonldData);
        db = await fluree.commit(ledger, stagedDb);
        console.log('✅ Data inserted and committed successfully!\n');
        
        // Step 5: Query all people
        console.log('5️⃣ Querying all people...');
        const peopleQuery = {
            '@context': {
                'schema': 'http://schema.org/',
                'ex': 'http://example.org/'
            },
            'select': {'?person': ['*']},
            'where': {
                '@id': '?person',
                '@type': 'schema:Person'
            }
        };
        
        const people = await fluree.query(db, peopleQuery);
        console.log('📊 All people:');
        console.log(JSON.stringify(people, null, 2));
        console.log();
        
        // Step 6: Query company with employees
        console.log('6️⃣ Querying company information...');
        const companyQuery = {
            '@context': {
                'schema': 'http://schema.org/',
                'ex': 'http://example.org/'
            },
            'select': {'?org': ['*', {'ex:employees': ['*']}]},
            'where': {
                '@id': '?org',
                '@type': 'schema:Organization'
            }
        };
        
        const company = await fluree.query(db, companyQuery);
        console.log('📊 Company information:');
        console.log(JSON.stringify(company, null, 2));
        console.log();
        
        // Step 7: Query by skills
        console.log('7️⃣ Querying people with JavaScript skills...');
        const skillsQuery = {
            '@context': {
                'schema': 'http://schema.org/',
                'ex': 'http://example.org/'
            },
            'select': ['?person', '?name'],
            'where': {
                '@id': '?person',
                '@type': 'schema:Person',
                'schema:name': '?name',
                'ex:skills': 'JavaScript'
            }
        };
        
        const jsDevs = await fluree.query(db, skillsQuery);
        console.log('📊 People with JavaScript skills:');
        console.log(JSON.stringify(jsDevs, null, 2));
        console.log();
        
        // Step 8: Update data
        console.log('8️⃣ Updating Alice\'s age...');
        const updateData = {
            '@context': {
                'schema': 'http://schema.org/',
                'ex': 'http://example.org/'
            },
            'delete': [
                {
                    '@id': 'ex:person1',
                    'schema:age': 30
                }
            ],
            'insert': [
                {
                    '@id': 'ex:person1',
                    'schema:age': 31
                }
            ]
        };
        
        const updatedDb = await fluree.stage(db, updateData);
        db = await fluree.commit(ledger, updatedDb);
        console.log('✅ Update committed successfully!\n');
        
        // Step 9: Verify update
        console.log('9️⃣ Verifying update...');
        const verifyQuery = {
            '@context': {
                'schema': 'http://schema.org/',
                'ex': 'http://example.org/'
            },
            'select': ['?name', '?age'],
            'where': {
                '@id': 'ex:person1',
                'schema:name': '?name',
                'schema:age': '?age'
            }
        };
        
        const updatedPerson = await fluree.query(db, verifyQuery);
        console.log('📊 Updated person:');
        console.log(JSON.stringify(updatedPerson, null, 2));
        console.log();
        
        console.log('✨ Demo completed successfully!');
        
    } catch (error) {
        console.error('❌ Error:', error.message);
        process.exit(1);
    }
}

// Run the demo
runDemo();