var dqueue = dqueue || {}

dqueue.publish = function() {
    var fd = new FormData()
    $.each($('#contents'), function(i, obj) {
        fd.append('contents', obj.files[0])
    })
    fd.append('startTime', $('#startTime').val())

    var url = '/dqueue/v1/publish'

    if ($('#filters').val().trim().length > 0) {
        url += '/' + $('#filters').val().trim()
    }

    $.ajax({
      url: url,
      data: fd,
      processData: false,
      contentType: false,
      type: 'POST',
      success: function(data) {
        alert('Published')
      },
      error: function(data) {
        alert('Got error ==> ' + data)
      }
    })
}
