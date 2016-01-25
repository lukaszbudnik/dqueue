/*
 * Copyright (C) 2015-2016 Łukasz Budnik <lukasz.budnik@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
var dqueue = dqueue || {}

dqueue.publish = function() {
    var url = '/dqueue/v1'

    var fd = new FormData()

    var dependency = $('#dependency').val().trim()
    if (dependency.length > 0) {
        url += '/ordered'
        fd.append('dependency', dependency)
    }
    fd.append('startTime', $('#startTime').val())
    $.each($('#contents'), function(i, obj) {
        fd.append('contents', obj.files[0])
    })

    url += '/publish'

    var filters = $('#filters').val().trim()
    var filtersHeader = null
    if (filters.length > 0) {
        filtersHeader = { 'X-Dqueue-Filters': filters }
    }

    $.ajax({
      url: url,
      data: fd,
      headers: filtersHeader,
      processData: false,
      contentType: false,
      type: 'POST',
      success: function(data) {
        alert('Published to ==> ' + url)
      },
      error: function(data) {
        alert('Got error ==> ' + data)
      }
    })
}

