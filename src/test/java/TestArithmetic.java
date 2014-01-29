
import com.connell.utils.Arithmetic;
import org.junit.Assert;
import org.junit.Test;

public class TestArithmetic {

  @Test
  public void testHighestOneBit() {
    Assert.assertEquals(524288, Integer.highestOneBit(1000000));
    Assert.assertEquals(16, Integer.highestOneBit(16));
    Assert.assertEquals(1048576, Integer.highestOneBit(1048576));
    Assert.assertEquals(524288, Integer.highestOneBit(1048575));
    Assert.assertEquals(1048576, Integer.highestOneBit(1100000));
  }

  @Test
  public void testBinExpMod() {
    Assert.assertEquals(8, Arithmetic.binExpMod(2, 3, 10));
    Assert.assertEquals(0, Arithmetic.binExpMod(2, 3, 8));
    Assert.assertEquals(3, Arithmetic.binExpMod(2, 3, 5));
    Assert.assertEquals(25, Arithmetic.binExpMod(3, 17, 37));
    Assert.assertEquals(2777434, Arithmetic.binExpMod(16, 1000000, (8*1000000)+1));
    Assert.assertEquals(0, Arithmetic.binExpMod(2, 2, 1));
    Assert.assertEquals(4, Arithmetic.binExpMod(16, 2, 9));
  }

  @Test
  public void testBinExp() {
    Assert.assertEquals(8, Arithmetic.binExp(2, 3));
    Assert.assertEquals(129140163, Arithmetic.binExp(3, 17));
  }

  @Test
  public void testFrac() {
    Assert.assertEquals(0.84, Arithmetic.frac(42.84), 0.00000001);
  }


}
