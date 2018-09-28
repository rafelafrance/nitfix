document.querySelector('tbody')
  .addEventListener('click', function(event) {
    if (! event.target.matches('button')) { return; }
    const cls = event.target.classList;
    if (cls.contains('family-btn') && cls.contains('closed')) {
      openFamily(event.target.dataset.family);
    } else if (cls.contains('family-btn')) {
      closeFamily(event.target.dataset.family);
    } else if (cls.contains('closed')) {
      openGenus(event.target.dataset.genus);
    } else {
      closeGenus(event.target.dataset.genus);
    }
});

function closeFamily(family) {
  const selector = `[data-family="${family}"]`;
  const elts = document.querySelectorAll(selector);
  elts.forEach(function(e) { e.classList.add('closed'); });
}

function openFamily(family) {
  const selector = `[data-family="${family}"]`;
  const elts = document.querySelectorAll(selector);
  elts.forEach(function(e) {
    if (e.classList.contains('genus')) {
      e.classList.remove('closed');
    } else if (e.classList.contains('family-btn')) {
      e.classList.remove('closed');
    } else if (e.dataset.closed) {
      e.classList.add('closed');
    } else {
      e.classList.remove('closed');
    }
  });
}

function closeGenus(genus) {
  const selector = `[data-genus="${genus}"]`;
  const elts = document.querySelectorAll(selector);
  elts.forEach(function(e) {
    e.classList.add('closed');
    e.dataset.closed = 'closed';
  });
}

function openGenus(genus) {
  const selector = `[data-genus="${genus}"]`;
  const elts = document.querySelectorAll(selector);
  elts.forEach(function(e) {
    e.classList.remove('closed');
    e.dataset.closed = '';
  });
}
