var TISSUE_ID_COL = 6;

var ERROR_BG = '#ff5555';
var OK_BG = '#ffffff';


function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Checks')
    .addItem('Find duplicate tissue IDs', 'markDuplicates')
    .addToUi();
}

function markDuplicates() {
  const sheet = SpreadsheetApp.getActiveSheet();
  const lastRow = sheet.getLastRow();
  var range = sheet.getRange(2, TISSUE_ID_COL, lastRow - 1);
  range.setBackground(OK_BG);

  const allIds = getAllIds();
  const dupes = findDuplicates(allIds);

  dupes.forEach(function(row) {
    range = sheet.getRange(row, TISSUE_ID_COL);
    range.setBackground(ERROR_BG);
  });

  if (dupes.length) {
    throw 'Duplicate tissue ID on rows: ' + dupes.join(', ');
  }
}


function findDuplicates(allIds) {
  var dupes = [];
  Object.keys(allIds).forEach(function(id) {
    if (allIds[id].length > 1) {
      dupes = dupes.concat(allIds[id]);
    }
  });
  return dupes.sort(function(a, b) { return a - b; });
}


function getAllIds() {
  const allIds = {};
  SpreadsheetApp.getActiveSheet()
    .getDataRange()
    .getValues()
    .forEach(function(row, r) {
      getIds(row[TISSUE_ID_COL - 1]).forEach(function(id) {
        allIds[id] = allIds[id] || [];
        allIds[id].push(r + 1);
      });
    });
  return allIds;
}


function getIds(contents) {
  return contents.split(';')
    .map(function(id) { return id.trim(); })
    .filter(function(id) { return isUuid(id); });
}


function isUuid(uuid) {
  return uuid.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
}
