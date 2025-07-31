import fluree from '../../out/fluree-browser-sdk.js';

// Global state
let connection = null;
let ledger = null;
let currentDb = null;

// Utility functions
function log(message, type = 'info') {
    const output = document.getElementById('output');
    const timestamp = new Date().toLocaleTimeString();
    const prefix = type === 'error' ? '❌' : type === 'success' ? '✅' : 'ℹ️';
    output.textContent += `[${timestamp}] ${prefix} ${message}\n`;
    output.scrollTop = output.scrollHeight;
}

function updateStatus(elementId, status, message) {
    const element = document.getElementById(elementId);
    element.className = `status ${status}`;
    element.textContent = `Status: ${message}`;
}

function enableButton(buttonId) {
    document.getElementById(buttonId).disabled = false;
}

function disableButton(buttonId) {
    document.getElementById(buttonId).disabled = true;
}

// Step 1: Create connection
window.createConnection = async function(type) {
    try {
        log(`Creating ${type} connection...`);
        disableButton('connectMemoryBtn');
        disableButton('connectLocalStorageBtn');
        
        if (type === 'memory') {
            connection = await fluree.connectMemory({});
            log('✅ Memory connection created successfully!', 'success');
            updateStatus('connectionStatus', 'success', 'Connected to Fluree (Memory)');
        } else if (type === 'localStorage') {
            connection = await fluree.connectLocalStorage({
                storageId: 'fluree-demo-storage',
                cacheMaxMb: 50
            });
            log('✅ LocalStorage connection created successfully!', 'success');
            updateStatus('connectionStatus', 'success', 'Connected to Fluree (LocalStorage)');
        }
        
        enableButton('createLedgerBtn');
        
    } catch (error) {
        log(`❌ Connection failed: ${error.message}`, 'error');
        updateStatus('connectionStatus', 'error', `Connection failed: ${error.message}`);
        enableButton('connectMemoryBtn');
        enableButton('connectLocalStorageBtn');
    }
};

// Step 2: Create ledger
window.createLedger = async function() {
    try {
        log('Creating ledger "demo-ledger"...');
        disableButton('createLedgerBtn');
        
        ledger = await fluree.create(connection, 'demo-ledger');
        currentDb = fluree.db(ledger);
        
        log('✅ Ledger created successfully!', 'success');
        updateStatus('ledgerStatus', 'success', 'Ledger "demo-ledger" created');
        enableButton('insertBtn');
        
    } catch (error) {
        log(`❌ Ledger creation failed: ${error.message}`, 'error');
        updateStatus('ledgerStatus', 'error', `Ledger creation failed: ${error.message}`);
        enableButton('createLedgerBtn');
    }
};

// Step 3: Insert data
window.insertData = async function() {
    try {
        const jsonldText = document.getElementById('jsonldData').value;
        const jsonldData = JSON.parse(jsonldText);
        
        log('Inserting JSON-LD data...');
        disableButton('insertBtn');
        
        const newDb = await fluree.stage(currentDb, jsonldData);
        currentDb = await fluree.commit(ledger, newDb);
        
        log('✅ Data inserted and committed successfully!', 'success');
        log(`Inserted ${jsonldData.insert.length} entities`, 'success');
        updateStatus('insertStatus', 'success', `${jsonldData.insert.length} entities inserted`);
        
        // Enable query buttons
        enableButton('queryAllBtn');
        enableButton('queryCompanyBtn');
        enableButton('querySkillsBtn');
        enableButton('customQueryBtn');
        enableButton('insertBtn');
        
    } catch (error) {
        log(`❌ Data insertion failed: ${error.message}`, 'error');
        updateStatus('insertStatus', 'error', `Insertion failed: ${error.message}`);
        enableButton('insertBtn');
    }
};

// Query functions
window.queryAllPeople = async function() {
    try {
        log('Querying all people...');
        
        const query = {
            "@context": {
                "schema": "http://schema.org/",
                "ex": "http://example.org/"
            },
            "select": {"?person": ["*"]},
            "where": {
                "@id": "?person",
                "@type": "schema:Person"
            }
        };
        
        const results = await fluree.query(currentDb, query);
        
        log('📊 Query Results - All People:');
        log(JSON.stringify(results, null, 2));
        
    } catch (error) {
        log(`❌ Query failed: ${error.message}`, 'error');
    }
};

window.queryCompany = async function() {
    try {
        log('Querying company information...');
        
        const query = {
            "@context": {
                "schema": "http://schema.org/",
                "ex": "http://example.org/"
            },
            "select": {"?org": ["*", {"ex:employees": ["*"]}]},
            "where": {
                "@id": "?org",
                "@type": "schema:Organization"
            }
        };
        
        const results = await fluree.query(currentDb, query);
        
        log('📊 Query Results - Company:');
        log(JSON.stringify(results, null, 2));
        
    } catch (error) {
        log(`❌ Query failed: ${error.message}`, 'error');
    }
};

window.queryBySkills = async function() {
    try {
        log('Querying people with JavaScript skills...');
        
        const query = {
            "@context": {
                "schema": "http://schema.org/",
                "ex": "http://example.org/"
            },
            "select": ["?person", "?name"],
            "where": {
                "@id": "?person",
                "@type": "schema:Person",
                "schema:name": "?name",
                "ex:skills": "JavaScript"
            }
        };
        
        const results = await fluree.query(currentDb, query);
        
        log('📊 Query Results - People with JavaScript skills:');
        log(JSON.stringify(results, null, 2));
        
    } catch (error) {
        log(`❌ Query failed: ${error.message}`, 'error');
    }
};

window.executeCustomQuery = async function() {
    try {
        const queryText = document.getElementById('customQuery').value;
        const query = JSON.parse(queryText);
        
        log('Executing custom query...');
        log('Query: ' + JSON.stringify(query, null, 2));
        
        const results = await fluree.query(currentDb, query);
        
        log('📊 Custom Query Results:');
        log(JSON.stringify(results, null, 2));
        
    } catch (error) {
        log(`❌ Custom query failed: ${error.message}`, 'error');
    }
};

// Initial setup
log('🚀 Fluree Browser SDK Demo loaded');
log('Choose "Memory" for temporary storage or "LocalStorage" for persistent storage...');