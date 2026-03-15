import { Moon, Sun, Monitor } from "lucide-react";
import { useTheme } from "next-themes";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

type ThemeToggleProps = {
  showLabel?: boolean;
  className?: string;
  align?: "start" | "center" | "end";
};

export function ThemeToggle({ showLabel = false, className, align = "end" }: ThemeToggleProps) {
  const { setTheme, theme, resolvedTheme } = useTheme();
  const [mounted, setMounted] = useState(false);

  // Avoid hydration mismatch
  useEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted) {
    return (
      <Button
        variant={showLabel ? "outline" : "ghost"}
        size={showLabel ? "sm" : "icon"}
        className={cn(showLabel ? "justify-start gap-2" : "h-9 w-9", className)}
      >
        <Sun className="h-4 w-4" />
        {showLabel ? <span>Theme</span> : null}
      </Button>
    );
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant={showLabel ? "outline" : "ghost"}
          size={showLabel ? "sm" : "icon"}
          className={cn(showLabel ? "justify-start gap-2" : "h-9 w-9", className)}
        >
          {resolvedTheme === "dark" ? (
            <Moon className="h-4 w-4" />
          ) : (
            <Sun className="h-4 w-4" />
          )}
          {showLabel ? (
            <span className="truncate">
              {theme === "system"
                ? "Theme: System"
                : resolvedTheme === "dark"
                  ? "Theme: Dark"
                  : "Theme: Light"}
            </span>
          ) : null}
          <span className="sr-only">Toggle theme</span>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align={align} className="z-[10000]">
        <DropdownMenuItem onClick={() => setTheme("light")}>
          <Sun className="mr-2 h-4 w-4" />
          Light
        </DropdownMenuItem>
        <DropdownMenuItem onClick={() => setTheme("dark")}>
          <Moon className="mr-2 h-4 w-4" />
          Dark
        </DropdownMenuItem>
        <DropdownMenuItem onClick={() => setTheme("system")}>
          <Monitor className="mr-2 h-4 w-4" />
          System
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
