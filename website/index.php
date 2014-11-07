<!DOCTYPE html>
<!-- saved from url=(0040)http://getbootstrap.com/examples/cover/# -->
<html lang="en"><head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate"/>
  <meta http-equiv="Pragma" content="no-cache"/>
  <meta http-equiv="Expires" content="0"/>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="description" content="">
    <meta name="author" content="">
    <link rel="icon" href="http://www.clipartlord.com/wp-content/uploads/2013/04/elephant8.png">

    <link rel="stylesheet" type="text/css" href="style.css" />
    <link rel="stylesheet" type="text/css" href="loading.css" />
    <script src="https://cdn.firebase.com/js/client/2.0.1/firebase.js"></script>
    <title>The Large NGram Collider</title>

    <!-- Bootstrap core CSS -->
    <link href="http://getbootstrap.com/dist/css/bootstrap.min.css" rel="stylesheet">

    <!-- Custom styles for this template -->
    <link href="http://getbootstrap.com/examples/cover/cover.css" rel="stylesheet">

    <!-- Just for debugging purposes. Don't actually copy these 2 lines! -->
    <!--[if lt IE 9]><script src="../../assets/js/ie8-responsive-file-warning.js"></script><![endif]-->
    <script src="./index_files/ie-emulation-modes-warning.js"></script>

    <!-- HTML5 shim and Respond.js for IE8 support of HTML5 elements and media queries -->
    <!--[if lt IE 9]>
      <script src="https://oss.maxcdn.com/html5shiv/3.7.2/html5shiv.min.js"></script>
      <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
    <![endif]-->

    <?php
      if ($_GET['submitted'] == 1) {
        $url = "hadoop26.csse.rose-hulman.edu:8002";
        $phrase = $_GET['words'];

        $data = array("phrase" => $_GET['words'], "algorithm" => $_GET['alg']);
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_setopt($ch, CURLOPT_POSTFIELDS,http_build_query($data));

        $response = curl_exec($ch);
      }
    ?>
<script type="text/javascript" src="http://code.jquery.com/jquery-1.6.3.min.js"></script>
<script type="text/javascript" src="scripts/jquery.cycle.all.2.74.js"></script>
    <script>
      $(document).ready(function() {
        $('#slideshow').cycle({
        fx: 'fade',
        pager: '#smallnav', 
        pause:   1, 
        speed: 1800,
        timeout:  3500 
      });     
    });
    </script>

  </head>

  <body>
    
        <script src="./scripts/fb.js"></script>
    </script>
    <div id="slideshow">
      <img src="skyline.jpg" class="bgM"/>
      <img src="skyline2.jpg" class="bgM"/>
      <img src="skyline3.jpg" class="bgM"/>
      <img src="skyline4.jpg" class="bgM"/>
      <img src="skyline5.jpg" class="bgM"/>
    </div>
    <div class="site-wrapper">

      <div class="site-wrapper-inner">

        <div class="cover-container">

          <div class="masthead clearfix">
            <div class="inner">
              <h3 class="masthead-brand"><?php $bullshit = array("Powerful", "Precise", "Stunning", "Innovative", "Massive", "Advanced", "Stylish", "Fresh");
                  $rand1 = rand(0,7);
                  $rand2 = $rand1;
                  while ($rand2 == $rand1) {
                    $rand2 = rand(0,7);
                  }
                  $output = $bullshit[$rand1] . ". " . $bullshit[$rand2] . ". ";
                  echo $output;
              ?>Mahout.</h3>
            </div>
          </div>

          <div class="inner cover">
       <!--     <h1 class="cover-heading">Type a phrase:</h1>
           <p class="lead">Cover is a one-page template for building simple and beautiful home pages. Download, edit the text, and add your own fullscreen background photo to make it your own.</p> -->
            
            <div class="container dark-matter">

            <h1>Reticulating Splines</h1> 
              <div id="circularG">
              <div id="circularG_1" class="circularG">
              </div>
              <div id="circularG_2" class="circularG">
              </div>
              <div id="circularG_3" class="circularG">
              </div>
              <div id="circularG_4" class="circularG">
              </div>
              <div id="circularG_5" class="circularG">
              </div>
              <div id="circularG_6" class="circularG">
              </div>
              <div id="circularG_7" class="circularG">
              </div>
              <div id="circularG_8" class="circularG">
              </div>
              </div>
            </div>

            <div id="resultDiv" class="dark-matter">
              <p id="queryString"></p><br />
              <h1 id="firstResult"></h1>
              <h2 id="secondResult"></h2>
              <h3 id="thirdResult"></h3>
              <div id="fireworks"></div>
            </div>
            
            <form id="initialForm" class="dark-matter" action="index.php" method="GET">
              <input type="text" name="words" value="The quick brown fox jumps over the lazy dog" onfocus="this.value = '';"/>
              <input type="hidden" name='submitted' value="1" />
              <input type="submit" value="Go!" class="button"/><br />
              <p class="lead">Naive Bayes</p>
              <input type="radio" style="margin-right:30px;" name="alg" value="NBayes" />
              <p class="lead">Okapi BM25</p>
              <input type="radio" name="alg" value="BM25" checked/><br />
              <p class="lead">Normalized K-Nearest Neighbor</p>
              <input type="radio" name="alg" value="knn" />
            </form>
          </div>

          <div class="mastfoot">
            <div class="inner">
              <p>&copy Man Hanson LLC., 2014.</p>
            </div>
          </div>

        </div>

      </div>

    </div>

    <!-- Bootstrap core JavaScript
    ================================================== -->
    <!-- Placed at the end of the document so the pages load faster -->
    <script src="./index_files/jquery.min.js"></script>
    <script src="./index_files/bootstrap.min.js"></script>
    <script src="./index_files/docs.min.js"></script>
    <!-- IE10 viewport hack for Surface/desktop Windows 8 bug -->
    <script src="./index_files/ie10-viewport-bug-workaround.js"></script>
  

<div id="global-zeroclipboard-html-bridge" class="global-zeroclipboard-container" style="position: absolute; left: 0px; top: -9999px; width: 15px; height: 15px; z-index: 999999999;" title="" data-original-title="Copy to clipboard">      <object classid="clsid:d27cdb6e-ae6d-11cf-96b8-444553540000" id="global-zeroclipboard-flash-bridge" width="100%" height="100%">         <param name="movie" value="/assets/flash/ZeroClipboard.swf?noCache=1415223893503">         <param name="allowScriptAccess" value="sameDomain">         <param name="scale" value="exactfit">         <param name="loop" value="false">         <param name="menu" value="false">         <param name="quality" value="best">         <param name="bgcolor" value="#ffffff">         <param name="wmode" value="transparent">         <param name="flashvars" value="trustedOrigins=getbootstrap.com%2C%2F%2Fgetbootstrap.com%2Chttp%3A%2F%2Fgetbootstrap.com">         <embed src="/assets/flash/ZeroClipboard.swf?noCache=1415223893503" loop="false" menu="false" quality="best" bgcolor="#ffffff" width="100%" height="100%" name="global-zeroclipboard-flash-bridge" allowscriptaccess="sameDomain" allowfullscreen="false" type="application/x-shockwave-flash" wmode="transparent" pluginspage="http://www.macromedia.com/go/getflashplayer" flashvars="trustedOrigins=getbootstrap.com%2C%2F%2Fgetbootstrap.com%2Chttp%3A%2F%2Fgetbootstrap.com" scale="exactfit">                </object></div></body></html>