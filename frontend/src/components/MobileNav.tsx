import { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { 
  Menu, Scan, Package, Library, 
  BarChart2, Settings, Wrench
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from '@/components/ui/sheet';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';

interface NavItem {
  to: string;
  icon: React.ElementType;
  label: string;
  badge?: number;
  badgeVariant?: 'default' | 'destructive' | 'warning';
}

// Simplified navigation - Tag Fixer and Incomplete Albums removed
const navItems: NavItem[] = [
  { to: '/', icon: Scan, label: 'Scan' },
  { to: '/library', icon: Library, label: 'Library' },
  { to: '/statistics', icon: BarChart2, label: 'Statistics' },
  { to: '/tools', icon: Wrench, label: 'Tools' },
];

interface MobileNavProps {
  duplicateCount?: number;
  onSettingsClick?: () => void;
}

export function MobileNav({ duplicateCount, onSettingsClick }: MobileNavProps) {
  const [open, setOpen] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();

  const handleNavigate = (to: string) => {
    navigate(to);
    setOpen(false);
  };

  // Add badges to nav items
  const itemsWithBadges = navItems.map(item => {
    if (item.to === '/tools' && duplicateCount) {
      return { ...item, badge: duplicateCount, badgeVariant: 'warning' as const };
    }
    return item;
  });

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        <Button variant="ghost" size="icon" className="lg:hidden">
          <Menu className="h-5 w-5" />
          <span className="sr-only">Toggle navigation menu</span>
        </Button>
      </SheetTrigger>
      <SheetContent side="left" className="w-72 p-0">
        <SheetHeader className="p-4 border-b border-border">
          <SheetTitle className="flex items-center gap-2 text-left">
            <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-primary text-primary-foreground">
              <Package className="w-4 h-4" />
            </div>
            <span>PMDA</span>
          </SheetTitle>
        </SheetHeader>
        
        <nav className="flex flex-col p-2">
          {itemsWithBadges.map((item) => {
            const Icon = item.icon;
            const isActive = item.to === '/library'
              ? location.pathname === '/library' || location.pathname.startsWith('/library/')
              : location.pathname === item.to;
            
            return (
              <button
                key={item.to}
                onClick={() => handleNavigate(item.to)}
                className={cn(
                  "flex items-center gap-3 px-3 py-2.5 rounded-lg text-small font-medium transition-colors",
                  "hover:bg-accent hover:text-accent-foreground",
                  isActive && "bg-accent text-accent-foreground"
                )}
              >
                <Icon className="w-5 h-5" />
                <span className="flex-1 text-left">{item.label}</span>
                {item.badge && item.badge > 0 && (
                  <Badge 
                    variant={item.badgeVariant === 'warning' ? 'outline' : 'destructive'}
                    className={cn(
                      "text-caption",
                      item.badgeVariant === 'warning' && "border-warning text-warning"
                    )}
                  >
                    {item.badge}
                  </Badge>
                )}
              </button>
            );
          })}
          
          <div className="h-px bg-border my-2" />
          
          <button
            onClick={() => {
              setOpen(false);
              onSettingsClick?.();
            }}
            className="flex items-center gap-3 px-3 py-2.5 rounded-lg text-small font-medium transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            <Settings className="w-5 h-5" />
            <span>Settings</span>
          </button>
        </nav>
      </SheetContent>
    </Sheet>
  );
}

/** Bottom navigation for mobile - 5 key items */
export function MobileBottomNav({ duplicateCount }: { duplicateCount?: number }) {
  const navigate = useNavigate();
  const location = useLocation();

  const bottomItems: NavItem[] = [
    { to: '/', icon: Scan, label: 'Scan' },
    { to: '/library', icon: Library, label: 'Library' },
    { to: '/tools', icon: Wrench, label: 'Tools', badge: duplicateCount, badgeVariant: 'warning' },
    { to: '/statistics', icon: BarChart2, label: 'Stats' },
  ];

  return (
    <nav className="fixed bottom-0 left-0 right-0 z-50 md:hidden border-t border-border bg-card/95 backdrop-blur supports-[backdrop-filter]:bg-card/80">
      <div className="flex items-center justify-around py-2">
        {bottomItems.map((item) => {
          const Icon = item.icon;
          const isActive = item.to === '/library'
            ? location.pathname === '/library' || location.pathname.startsWith('/library/')
            : location.pathname === item.to;
          
          return (
            <button
              key={item.to}
              onClick={() => navigate(item.to)}
              className={cn(
                "relative flex flex-col items-center gap-1 px-3 py-1.5 rounded-lg transition-colors",
                isActive ? "text-primary" : "text-muted-foreground hover:text-foreground"
              )}
            >
              <div className="relative">
                <Icon className="w-5 h-5" />
                {item.badge && item.badge > 0 && (
                  <span className={cn(
                    "absolute -top-1.5 -right-1.5 min-w-[16px] h-4 px-1 rounded-full text-[10px] font-medium flex items-center justify-center",
                    item.badgeVariant === 'warning' 
                      ? "bg-warning text-warning-foreground"
                      : "bg-destructive text-destructive-foreground"
                  )}>
                    {item.badge > 99 ? '99+' : item.badge}
                  </span>
                )}
              </div>
              <span className="text-[10px] font-medium">{item.label}</span>
            </button>
          );
        })}
      </div>
    </nav>
  );
}
