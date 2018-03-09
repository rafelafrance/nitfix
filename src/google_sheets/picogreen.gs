var PLATE_SHEET_ID = '1uPOtAuu3VQUcVkRsY4q2GtfeFrs1l9udkyNEaxvqjmA';
var LOCAL_ID_OFFSET = 2;  // Add this to get to the date cell
var TABLE_OFFSET = 6;     // Add this to get to the top of the sample table
var TABLE_END = 14;       // Add this to get pass the end of the sample table
var PICOGREEN_ID_IDX = 0;
var UUID_IDX = 7;
var UUID_COL = UUID_IDX + 1;


function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Update')
    .addItem('Update UUIDs', 'updateUuids')
    .addToUi();
}


function updateUuids() {
  const sampleIds = getSampleIds();
  const uuids = [];
  const sheet = SpreadsheetApp.getActiveSheet();
  sheet.getDataRange()
    .getValues()
    .forEach(function (row, r) {
      const sampleId = sampleIds[row[PICOGREEN_ID_IDX]] || row[UUID_IDX] || '';
      uuids.push( [sampleId] );
    });

 sheet.getRange(1, UUID_COL, uuids.length, 1)
   .setValues(uuids);
}

function getSampleIds() {
  const localIdRegex = /^.*\D(\d+)$/;
  const sampleIds = {};
  const rows = SpreadsheetApp
    .openById(PLATE_SHEET_ID)
    .getActiveSheet()
    .getDataRange()
    .getValues();

  var r = 0;
  var i = 0;
  while(r < rows.length) {
    var row = rows[r];
    if (isUuid(row[0])) {
      var localId = rows[r + LOCAL_ID_OFFSET][0];
      plateNo = localIdRegex.exec(localId)[1];
      var well = 0;
      for (i = r + TABLE_OFFSET; i < r + TABLE_END; ++i) {
        row = rows[i];
        for (j = 1; j < 13; ++j) {
          well += 1;
          var picogreenId = plateNo + '_' + ('00' + well).slice(-2);
          sampleIds[picogreenId] = row[j];
        }
      }
      r += TABLE_END;
    } else {
      r += 1;
    }
  }
  return sampleIds;
}


function isUuid(uuid) {
  if (typeof uuid !== 'string') {
    return false;
  }
  return !!uuid.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
}
