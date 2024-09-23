#include "re.h"
#include "stm.h"


char read_wite(char &b, int &c){
  char h = read(c + stm_run());
  c = encode(b);
  return h;
}