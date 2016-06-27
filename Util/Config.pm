package Util::Config;

use Mojo::Asset::File;

sub get_config {
  my $path = 'config';
  my $file = Mojo::Asset::File->new(path => $path);
  my $config = eval $file->slurp;
  return $config;
}

1;
