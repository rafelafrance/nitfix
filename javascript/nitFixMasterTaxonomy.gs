var TISSUE_ID_COL = 6;

var ERROR_BG = '#ff5555';
var OK_BG = '#ffffff';


function onEdit() {
  const sheet = SpreadsheetApp.getActiveSheet();
  const cell = sheet.getActiveCell();
  const col = cell.getColumn();

  if (col != TISSUE_ID_COL) { return; }

  const value = cell.getDisplayValue();
  const row = cell.getRow();
  const allIds = {};
  const newIds = getIds(value);

  sheet.getDataRange()
    .getValues()
    .forEach(function(row, r) {
      getIds(row[TISSUE_ID_COL - 1]).forEach(function(id) {
        allIds[id] = allIds[id] || [];
        allIds[id].push(r + 1);
      });
    });

  Object.keys(allIds).forEach(function(id) {
    row
    if (allIds[id].length > 1) {
      
    }
  });

  // cell.setBackground(OK_BG);
  // newIds.forEach(function (id) {
  //   if (allIds[id].length > 1) {
  //     cell.setBackground(ERROR_BG);
  //   }
  // });
}


function getIds(contents) {
  return contents.split(';')
    .map(function(id) { return id.trim(); })
    .filter(function(id) { return isUuid(id); });
}


function isUuid(uuid) {
  return uuid.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
}
