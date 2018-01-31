////////////////////////////////////////////////////////////////////////////////////
// section
document.querySelectorAll('h2').forEach(function(header) {
  header.addEventListener('click', function(event) {
    event.target.classList.toggle('closed');
    const selector = '.' + event.target.dataset.selector;
    document.querySelector(selector).classList.toggle('closed');
  });
});

////////////////////////////////////////////////////////////////////////////////////
// section.families

// Toggle a group of rows open/closed. That is we will show/hide all genus records
// for a family. Closing them leaves only the first record (the family one) visible.
document.querySelector('.families tbody')
  .addEventListener('click', function(event) {
    if (! event.target.matches('button')) { return; }
    event.target.classList.toggle('closed');
    const family = event.target.dataset.family;
    const selector = '.families tr[data-family="' + family + '"]';
    const trs = document.querySelectorAll(selector);
    trs.forEach(function(r) { r.classList.toggle('closed'); });
});


////////////////////////////////////////////////////////////////////////////////////
// section.plates

const plates = {{ plates | safe }};
const wells = {{ wells | safe }};
var maxPage = plates.length;

function filterChange() {
  // maxPage = Math.ceil(filters[filter].length / args.page_size);
  document.querySelector('.pager').setAttribute('max', maxPage);
  document.querySelector('.max-page').innerHTML = 'of ' + maxPage;
  changePage();
}

document.querySelectorAll('.search').forEach(function(search) {
  search.addEventListener('keyup', function(event) {
    console.log(event.target.value);
  });
});

function buildTable(rows) {
  const tbody = document.querySelector('section.plates tbody');

  // Remove old rows
  while (tbody.firstChild) {
    tbody.removeChild(tbody.firstChild);
  }

  // Add new rows
  rows.forEach(function(row) {

    // Build the table's rows
    const tr = document.createElement('tr');
    if (row.cls) { tr.classList.add(row.cls); }

    // Build the row's cells
    row.td.forEach(function(cell) {
      const td = document.createElement('td');
      if (cell.cls) { td.classList.add(cell.cls); }
      td.innerHTML = cell.content;
      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });

}

function buildPlateHeader() {
  return {
    cls: 'header',
    td: [
      { content: 'Plate ID' },
      { content: 'Entry Date' },
      { content: 'Local ID' },
      { content: 'Protocol' },
      { content: 'Notes' },
    ],
  };
}

function buildPlateData(plate) {
  return {
    td: [
      { content: plate.plate_id, cls: 'l' },
      { content: plate.entry_date },
      { content: plate.local_id, cls: 'l' },
      { content: plate.protocol, cls: 'l' },
      { content: plate.notes,    cls: 'l' },
    ],
  };
}

function buildWellHeader() {
  return {
    cls: 'sub-header',
    td: [
      { content: '', cls: 'empty'},
      { content: 'Row' },
      { content: 'Column' },
      { content: 'Scientific Name' },
      { content: 'Sample ID' },
    ],
  };
}

function buildWellData(well) {
  return {
    cls: 'well',
    td: [
      { content: '', cls: 'empty' },
      { content: well.plate_row },
      { content: well.plate_col },
      { content: well.scientific_name, cls: 'l' },
      { content: well.sample_id,       cls: 'l' },
    ],
  };
}

function buildTableRows(page) {
  const rows = [];
  const plate = plates[page - 1];
  rows.push(buildPlateHeader());
  rows.push(buildPlateData(plate));
  rows.push(buildWellHeader());
  wells[plate.plate_id].forEach(function(well) {
    rows.push(buildWellData(well));
  });
  return rows;
}

function changePage() {
  const pager = document.querySelector('.pager');
  var page = +pager.value;
  page = page < 1 ? 1 : page;
  page = page > maxPage ? maxPage : page;
  pager.value = page;
  const rows = buildTableRows(page);
  buildTable(rows);
}

document.querySelector('.first-page').addEventListener('click', function() {
  const pager = document.querySelector('.pager');
  pager.value = 1;
  changePage();
});

document.querySelector('.previous-page').addEventListener('click', function() {
  const pager = document.querySelector('.pager');
  pager.value = +pager.value - 1;
  changePage();
});

document.querySelector('.next-page').addEventListener('click', function() {
  const pager = document.querySelector('.pager');
  pager.value = +pager.value + 1;
  changePage();
});

document.querySelector('.last-page').addEventListener('click', function() {
  const pager = document.querySelector('.pager');
  pager.value = maxPage;
  changePage();
});

filterChange();
