let generatedTitle = '';

function submitFormData() {
    // Get the value from each input box
    const photographer = document.getElementById('photographer').value;
    const topic = document.getElementById('topic').value;
    const siteSpecific = document.getElementById('siteSpecific').value;
    const siteOwner = document.getElementById('siteOwner').value;
    const county = document.getElementById('county').value;
    const state = document.getElementById('state').value;
    const partnerAffiliation = document.getElementById('partnerAffiliation').value;
    const date = document.getElementById('date').value;

    // Check if any field is empty or contains underscores
    if (!photographer || !topic || !siteSpecific || !siteOwner || !county || !state || !partnerAffiliation || !date) {
        document.getElementById('output').innerHTML = '<p style="color: red;">Error: All fields are required.</p>';
    } else if (
        photographer.includes('_') || topic.includes('_') || siteSpecific.includes('_') ||
        siteOwner.includes('_') || county.includes('_') || state.includes('_') || 
        partnerAffiliation.includes('_') || date.includes('_')
    ) {
        // Display error message
        document.getElementById('output').innerHTML = '<p style="color: red;">Error: Inputs cannot contain underscores (_).</p>';
    } else {
        // Generate a title using Photographer, Topic, and Date
        generatedTitle = `${photographer}_${topic}_${date}`;

        // Display the title in the output div
        document.getElementById('output').innerHTML = `<p>Title: ${generatedTitle}</p>`;
    }
}

function clearData() {
    // Clear the form fields
    document.getElementById('photographer').value = '';
    document.getElementById('topic').value = '';
    document.getElementById('siteSpecific').value = '';
    document.getElementById('siteOwner').value = '';
    document.getElementById('county').value = '';
    document.getElementById('state').value = '';
    document.getElementById('partnerAffiliation').value = '';
    document.getElementById('date').value = '';

    // Clear the output div
    document.getElementById('output').innerHTML = '';
    generatedTitle = '';
}

function copyTitleToClipboard() {
    if (generatedTitle) {
        // Copy the title to the clipboard
        navigator.clipboard.writeText(generatedTitle).then(() => {
            document.getElementById('output').innerHTML += '<p style="color: green;">Title copied to clipboard!</p>';
        }).catch(err => {
            document.getElementById('output').innerHTML += '<p style="color: red;">Failed to copy title!</p>';
        });
    } else {
        document.getElementById('output').innerHTML += '<p style="color: red;">No title to copy! Please submit the form first.</p>';
    }
}
