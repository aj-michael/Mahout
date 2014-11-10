$(function() {
	var fb = new Firebase("https://mahout.firebaseio.com/");
  $('.circle, .circle1').removeClass('stop'); 
	
  // Attach an asynchronous callback to read the data at our posts reference
	fb.on("value", function(snapshot) {
	  var data = snapshot.val();
	  if (data.status == "complete") {
	  	// Display the top three results #initialForm | #resultDiv
	  	$("#resultDiv").show();
	  	$('#initialForm').show();
      $('#loading').hide();

	  	$('#queryString').html("\"" + data.query + "\"");
	  	$('#firstResult').html("1. " + data.results[1]);
	  	$('#secondResult').html("2. " + data.results[2]);
	  	$('#thirdResult').html("3. " + data.results[3]);
	  } else {
	  	// Make a cutsie animation of a machine that does some shit
	  	$("#resultDiv").hide();
	  	$("#initialForm").hide();

      $('#loading').show();
	  }
	}, function (errorObject) {
	  console.log("The read failed: " + errorObject.code);
	});
});